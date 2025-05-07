use std::{any::Any, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{ExprRef, WindowExpr};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::ExecutionTaskSpawner;

pub struct WindowOrderByOnlyState {
    record_batches: Vec<RecordBatch>,
    window_exprs: Vec<WindowExpr>,
    aliases: Vec<String>,
    original_schema: SchemaRef,
    row_offset: u64,
}

impl WindowOrderByOnlyState {
    fn new(
        window_exprs: Vec<WindowExpr>,
        aliases: Vec<String>,
        original_schema: SchemaRef,
    ) -> Self {
        Self {
            record_batches: Vec::new(),
            window_exprs,
            aliases,
            original_schema,
            row_offset: 0,
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

impl IntermediateOpState for WindowOrderByOnlyState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct WindowOrderByOnlyOperator {
    order_by: Vec<ExprRef>,
    _descending: Vec<bool>,
    window_exprs: Vec<WindowExpr>,
    aliases: Vec<String>,
    original_schema: SchemaRef,
}

impl WindowOrderByOnlyOperator {
    pub fn new(
        window_exprs: &[WindowExpr],
        aliases: &[String],
        order_by: &[ExprRef],
        descending: &[bool],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            window_exprs: window_exprs.to_vec(),
            aliases: aliases.to_vec(),
            order_by: order_by.to_vec(),
            _descending: descending.to_vec(),
            original_schema: schema.clone(),
        })
    }
}

impl IntermediateOperator for WindowOrderByOnlyOperator {
    #[instrument(skip_all, name = "WindowOrderByOnlyOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn IntermediateOpState>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        let window_state = state
            .as_any_mut()
            .downcast_mut::<WindowOrderByOnlyState>()
            .expect("WindowOrderByOnlyOperator must own WindowOrderByOnlyState");

        window_state.push(input).unwrap();

        let start_offset = window_state.row_offset;

        let window_exprs = window_state.window_exprs.clone();
        let aliases = window_state.aliases.clone();
        let original_schema = window_state.original_schema.clone();
        let mut batches = window_state.drain_record_batches();

        task_spawner
            .spawn(
                async move {
                    let mut running_offset = start_offset;

                    let mut out_batches = Vec::with_capacity(batches.len());
                    for mut batch in batches.drain(..) {
                        if batch.is_empty() {
                            continue;
                        }

                        for (wexpr, name) in window_exprs.iter().zip(&aliases) {
                            match wexpr {
                                WindowExpr::RowNumber => {
                                    batch = batch
                                        .window_row_number_global(name.clone(), running_offset)?;
                                }
                                _ => {
                                    return Err(DaftError::NotImplemented(format!(
                                        "Unsupported window fn: {wexpr:?}"
                                    )))
                                }
                            }
                        }
                        running_offset += batch.len() as u64;
                        out_batches.push(batch);
                    }

                    if let Some(ws) = state.as_any_mut().downcast_mut::<WindowOrderByOnlyState>() {
                        ws.row_offset = running_offset;
                    }

                    let output = if out_batches.is_empty() {
                        MicroPartition::empty(Some(original_schema.clone()))
                    } else {
                        MicroPartition::new_loaded(
                            original_schema.clone(),
                            out_batches.into(),
                            None,
                        )
                    };

                    Ok((
                        state,
                        IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(output))),
                    ))
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
            self.window_exprs.iter().map(|e| e.to_string()).join(", ")
        ));
        if !self.order_by.is_empty() {
            display.push(format!(
                "Order by: {}",
                self.order_by.iter().map(|e| e.to_string()).join(", ")
            ));
        }
        display
    }

    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(WindowOrderByOnlyState::new(
            self.window_exprs.clone(),
            self.aliases.clone(),
            self.original_schema.clone(),
        )))
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(1)
    }
}
