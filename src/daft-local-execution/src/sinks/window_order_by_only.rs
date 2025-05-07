use std::{any::Any, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{resolved_col, ExprRef, WindowExpr};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::ExecutionTaskSpawner;

struct WindowOrderByOnlyParams {
    window_exprs: Vec<WindowExpr>,
    aliases: Vec<String>,
    order_by: Vec<ExprRef>,
    _descending: Vec<bool>,
    original_schema: SchemaRef,
}

struct WindowOrderByOnlyState {
    record_batches: Vec<RecordBatch>,
}

impl WindowOrderByOnlyState {
    fn new() -> Self {
        Self {
            record_batches: Vec::new(),
        }
    }

    fn push(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        let tables = input.get_tables()?;
        for i in 0..tables.len() {
            self.record_batches.push(tables[i].clone());
        }
        Ok(())
    }

    fn drain_record_batches(&mut self) -> Vec<RecordBatch> {
        std::mem::take(&mut self.record_batches)
    }
}

impl BlockingSinkState for WindowOrderByOnlyState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct WindowOrderByOnlySink {
    window_order_by_only_params: Arc<WindowOrderByOnlyParams>,
}

impl WindowOrderByOnlySink {
    pub fn new(
        window_exprs: &[WindowExpr],
        aliases: &[String],
        order_by: &[ExprRef],
        descending: &[bool],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            window_order_by_only_params: Arc::new(WindowOrderByOnlyParams {
                window_exprs: window_exprs.to_vec(),
                aliases: aliases.to_vec(),
                order_by: order_by.to_vec(),
                _descending: descending.to_vec(),
                original_schema: schema.clone(),
            }),
        })
    }
}

impl BlockingSink for WindowOrderByOnlySink {
    #[instrument(skip_all, name = "WindowOrderByOnlySink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        spawner
            .spawn(
                async move {
                    let window_state = state
                        .as_any_mut()
                        .downcast_mut::<WindowOrderByOnlyState>()
                        .unwrap_or_else(|| {
                            panic!("WindowOrderByOnlySink should have WindowOrderByOnlyState")
                        });
                    window_state.push(input)?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "WindowOrderByOnlySink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.window_order_by_only_params.clone();

        spawner
            .spawn(
                async move {
                    let mut all_batches: Vec<RecordBatch> = Vec::new();
                    for mut state_box in states {
                        let state = state_box
                            .as_any_mut()
                            .downcast_mut::<WindowOrderByOnlyState>()
                            .expect("WindowOrderByOnlySink should have WindowOrderByOnlyState");
                        all_batches.append(&mut state.drain_record_batches());
                    }

                    if all_batches.is_empty() {
                        let empty_result = MicroPartition::empty(Some(params.original_schema.clone()));
                        return Ok(Some(Arc::new(empty_result)));
                    }

                    let mut results = Vec::with_capacity(all_batches.len());
                    let mut row_offset: u64 = 0;

                    for batch in all_batches {
                        if batch.is_empty() {
                            continue;
                        }

                        let mut current_batch = batch;

                        for (window_expr, name) in params.window_exprs.iter().zip(params.aliases.iter()) {
                            current_batch = match window_expr {
                                WindowExpr::RowNumber => {
                                    current_batch.window_row_number_global(name.clone(), row_offset)?
                                }
                                _ => {
                                    return Err(DaftError::NotImplemented(format!(
                                        "WindowOrderByOnlySink: Window function {:?} is not yet supported",
                                        window_expr
                                    )));
                                }
                            }
                        }

                        row_offset += current_batch.len() as u64;

                        let all_projections = params
                            .original_schema
                            .field_names()
                            .map(resolved_col)
                            .collect::<Vec<_>>();

                        let final_batch = current_batch.eval_expression_list(&all_projections)?;
                        results.push(final_batch);
                    }

                    if results.is_empty() {
                        let empty_result = MicroPartition::empty(Some(params.original_schema.clone()));
                        return Ok(Some(Arc::new(empty_result)));
                    }

                    let final_result = MicroPartition::new_loaded(
                        params.original_schema.clone(),
                        results.into(),
                        None,
                    );

                    Ok(Some(Arc::new(final_result)))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "WindowOrderByOnly"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "WindowOrderByOnly: {}",
            self.window_order_by_only_params
                .window_exprs
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        if !self.window_order_by_only_params.order_by.is_empty() {
            display.push(format!(
                "Order by: {}",
                self.window_order_by_only_params
                    .order_by
                    .iter()
                    .map(|e| e.to_string())
                    .join(", ")
            ));
        }
        display
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(WindowOrderByOnlyState::new()))
    }
}
