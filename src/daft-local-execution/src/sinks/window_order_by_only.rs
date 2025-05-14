use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{expr::bound_expr::BoundExpr, ExprRef, WindowExpr};
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
};
use crate::ExecutionTaskSpawner;

struct WindowOrderByOnlyParams {
    window_exprs: Vec<WindowExpr>,
    aliases: Vec<String>,
    order_by: Vec<ExprRef>,
    descending: Vec<bool>,
    original_schema: SchemaRef,
}

pub struct WindowOrderByOnlySink {
    params: Arc<WindowOrderByOnlyParams>,
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
            params: Arc::new(WindowOrderByOnlyParams {
                window_exprs: window_exprs.to_vec(),
                aliases: aliases.to_vec(),
                order_by: order_by.to_vec(),
                descending: descending.to_vec(),
                original_schema: schema.clone(),
            }),
        })
    }
}

struct WindowOrderByOnlyState {
    partitions: Vec<Arc<MicroPartition>>,
}

impl WindowOrderByOnlyState {
    fn new() -> Self {
        Self {
            partitions: Vec::new(),
        }
    }

    fn push(&mut self, input: Arc<MicroPartition>, _sink_name: &str) -> DaftResult<()> {
        self.partitions.push(input);
        Ok(())
    }
}

impl BlockingSinkState for WindowOrderByOnlyState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
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
        let sink_name = self.name().to_string();
        spawner
            .spawn(
                async move {
                    let window_state = state
                        .as_any_mut()
                        .downcast_mut::<WindowOrderByOnlyState>()
                        .unwrap_or_else(|| {
                            panic!("{} should have WindowOrderByOnlyState", sink_name)
                        });

                    window_state.push(input, &sink_name)?;
                    Ok(super::blocking_sink::BlockingSinkStatus::NeedMoreInput(
                        state,
                    ))
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
        let params = self.params.clone();

        spawner
            .spawn(
                async move {
                    // Gather all partitions from all states
                    let all_partitions = states
                        .into_iter()
                        .flat_map(|mut state| {
                            let state = state
                                .as_any_mut()
                                .downcast_mut::<WindowOrderByOnlyState>()
                                .expect("WindowOrderByOnlySink should have WindowOrderByOnlyState");
                            std::mem::take(&mut state.partitions)
                        })
                        .collect::<Vec<_>>();

                    // Concatenate all partitions
                    let concatenated = MicroPartition::concat(all_partitions)?;

                    // Sort the concatenated partition by order_by
                    let sorted = concatenated.sort(
                        &params.order_by,
                        &params.descending,
                        &params.descending, // Use descending for nulls_first as well, matching the sort behavior
                    )?;

                    if sorted.is_empty() {
                        let empty_result =
                            MicroPartition::empty(Some(params.original_schema.clone()));
                        return Ok(Some(Arc::new(empty_result)));
                    }

                    // Convert to RecordBatch for window operations
                    let tables = sorted.get_tables()?;
                    let mut out_batches = Vec::with_capacity(tables.len());

                    // Process each batch with window functions
                    for batch in tables.iter().cloned() {
                        if batch.is_empty() {
                            continue;
                        }

                        let mut result_batch = batch;

                        // Prepare the order_by expressions for window functions
                        let order_by = params
                            .order_by
                            .iter()
                            .map(|expr| BoundExpr::try_new(expr.clone(), &result_batch.schema))
                            .collect::<DaftResult<Vec<_>>>()?;

                        // Apply each window expression
                        for (wexpr, name) in params.window_exprs.iter().zip(&params.aliases) {
                            result_batch = match wexpr {
                                WindowExpr::RowNumber => {
                                    result_batch.window_row_number(name.clone())?
                                }
                                WindowExpr::Rank => {
                                    result_batch.window_rank(name.clone(), &order_by, false)?
                                }
                                WindowExpr::DenseRank => {
                                    result_batch.window_rank(name.clone(), &order_by, true)?
                                }
                                _ => {
                                    return Err(DaftError::ValueError(
                                        format!(
                                            "Unsupported window function for order by only: {:?}",
                                            wexpr
                                        )
                                        .into(),
                                    ));
                                }
                            };
                        }
                        out_batches.push(result_batch);
                    }

                    // Create final output partition
                    let output = if out_batches.is_empty() {
                        MicroPartition::empty(Some(params.original_schema.clone()))
                    } else {
                        MicroPartition::new_loaded(
                            params.original_schema.clone(),
                            out_batches.into(),
                            None,
                        )
                    };

                    Ok(Some(Arc::new(output)))
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
        display.push(format!(
            "Order by: {}",
            self.params
                .order_by
                .iter()
                .zip(self.params.descending.iter())
                .map(|(e, d)| format!("{} {}", e, if *d { "desc" } else { "asc" }))
                .join(", ")
        ));
        display
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(WindowOrderByOnlyState::new()))
    }
}
