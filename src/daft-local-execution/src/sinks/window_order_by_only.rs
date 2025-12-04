use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_core::prelude::*;
use daft_dsl::{
    WindowExpr,
    expr::bound_expr::{BoundExpr, BoundWindowExpr},
};
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

struct WindowOrderByOnlyParams {
    window_exprs: Vec<BoundWindowExpr>,
    aliases: Vec<String>,
    order_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    original_schema: SchemaRef,
}

pub struct WindowOrderByOnlySink {
    params: Arc<WindowOrderByOnlyParams>,
}

impl WindowOrderByOnlySink {
    pub fn new(
        window_exprs: &[BoundWindowExpr],
        aliases: &[String],
        order_by: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            params: Arc::new(WindowOrderByOnlyParams {
                window_exprs: window_exprs.to_vec(),
                aliases: aliases.to_vec(),
                order_by: order_by.to_vec(),
                descending: descending.to_vec(),
                nulls_first: nulls_first.to_vec(),
                original_schema: schema.clone(),
            }),
        })
    }
}

pub(crate) struct WindowOrderByOnlyState {
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

impl BlockingSink for WindowOrderByOnlySink {
    type State = WindowOrderByOnlyState;
    #[instrument(skip_all, name = "WindowOrderByOnlySink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let sink_name = self.name().to_string();
        spawner
            .spawn(
                async move {
                    state.push(input, &sink_name)?;
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
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let params = self.params.clone();

        spawner
            .spawn(
                async move {
                    // Gather all partitions from all states
                    let all_partitions = states
                        .into_iter()
                        .flat_map(|mut state| std::mem::take(&mut state.partitions))
                        .collect::<Vec<_>>();

                    // Concatenate all partitions
                    let concatenated = MicroPartition::concat(all_partitions)?;

                    // Sort the concatenated partition by order_by
                    let sorted = concatenated.sort(
                        &params.order_by,
                        &params.descending,
                        &params.nulls_first,
                    )?;

                    if sorted.is_empty() {
                        let empty_result =
                            MicroPartition::empty(Some(params.original_schema.clone()));
                        return Ok(BlockingSinkFinalizeOutput::Finished(vec![Arc::new(
                            empty_result,
                        )]));
                    }

                    // Convert to RecordBatch for window operations
                    let tables = sorted.record_batches();
                    let mut out_batches = Vec::with_capacity(tables.len());

                    // Process each batch with window functions
                    for batch in tables.iter().cloned() {
                        if batch.is_empty() {
                            continue;
                        }

                        let mut result_batch = batch;

                        // Apply each window expression
                        for (wexpr, name) in params.window_exprs.iter().zip(&params.aliases) {
                            result_batch = match wexpr.as_ref() {
                                WindowExpr::RowNumber => {
                                    result_batch.window_row_number(name.clone())?
                                }
                                WindowExpr::Rank => result_batch.window_rank(
                                    name.clone(),
                                    &params.order_by,
                                    false,
                                )?,
                                WindowExpr::DenseRank => result_batch.window_rank(
                                    name.clone(),
                                    &params.order_by,
                                    true,
                                )?,
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

                    Ok(BlockingSinkFinalizeOutput::Finished(vec![Arc::new(output)]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "WindowOrderByOnly".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::WindowOrderByOnly
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
                .zip(self.params.nulls_first.iter())
                .map(|((e, d), n)| format!(
                    "{} {} {}",
                    e,
                    if *d { "desc" } else { "asc" },
                    if *n { "nulls first" } else { "nulls last" }
                ))
                .join(", ")
        ));
        display
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(WindowOrderByOnlyState::new())
    }
}
