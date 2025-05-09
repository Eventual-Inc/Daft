use std::{any::Any, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{ExprRef, WindowExpr};
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::ExecutionTaskSpawner;

#[derive(Clone)]
pub struct WindowOrderByOnlyParams {
    window_exprs: Vec<WindowExpr>,
    aliases: Vec<String>,
    original_schema: SchemaRef,
    order_by: Vec<ExprRef>,
    _descending: Vec<bool>,
}

pub struct WindowOrderByOnlyState {
    row_offset: u64,
}

impl WindowOrderByOnlyState {
    fn new() -> Self {
        Self { row_offset: 0 }
    }
}

impl IntermediateOpState for WindowOrderByOnlyState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct WindowOrderByOnlyOperator {
    params: Arc<WindowOrderByOnlyParams>,
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
            params: Arc::new(WindowOrderByOnlyParams {
                window_exprs: window_exprs.to_vec(),
                aliases: aliases.to_vec(),
                original_schema: schema.clone(),
                order_by: order_by.to_vec(),
                _descending: descending.to_vec(),
            }),
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

        let start_offset = window_state.row_offset;
        let params = self.params.clone();

        task_spawner
            .spawn(
                async move {
                    let mut running_offset = start_offset;

                    let tables = input.get_tables().unwrap();
                    let mut out_batches = Vec::with_capacity(tables.len());
                    for mut batch in tables.iter().cloned() {
                        if batch.is_empty() {
                            continue;
                        }

                        for (wexpr, name) in params.window_exprs.iter().zip(&params.aliases) {
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
                        MicroPartition::empty(Some(params.original_schema.clone()))
                    } else {
                        MicroPartition::new_loaded(
                            params.original_schema.clone(),
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
            self.params
                .window_exprs
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        if !self.params.order_by.is_empty() {
            display.push(format!(
                "Order by: {}",
                self.params
                    .order_by
                    .iter()
                    .map(|e| e.to_string())
                    .join(", ")
            ));
        }
        display
    }

    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(WindowOrderByOnlyState::new()))
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(1) // necessary due to single-threaded execution with a single state
    }
}
