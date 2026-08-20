use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_core::prelude::*;
use daft_dsl::{
    WindowExpr,
    expr::bound_expr::{BoundExpr, BoundWindowExpr},
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
};

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
    partitions: Vec<MicroPartition>,
}

impl WindowOrderByOnlyState {
    fn new() -> Self {
        Self {
            partitions: Vec::new(),
        }
    }

    fn push(&mut self, input: MicroPartition, _sink_name: &str) -> DaftResult<()> {
        self.partitions.push(input);
        Ok(())
    }
}

impl BlockingSink for WindowOrderByOnlySink {
    type State = WindowOrderByOnlyState;
    #[instrument(skip_all, name = "WindowOrderByOnlySink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let sink_name = self.name().to_string();
        spawner
            .spawn(
                async move {
                    state.push(input, &sink_name)?;
                    Ok(state)
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
    ) -> BlockingSinkFinalizeResult {
        let params = self.params.clone();

        spawner
            .spawn(
                async move {
                    let all_batches: Vec<RecordBatch> = states
                        .into_iter()
                        .flat_map(|state| {
                            state
                                .partitions
                                .into_iter()
                                .flat_map(|mp| mp.record_batches().to_vec())
                        })
                        .collect();

                    if all_batches.is_empty() {
                        return Ok(BlockingSinkOutput::Partitions(vec![MicroPartition::empty(
                            Some(params.original_schema.clone()),
                        )]));
                    }

                    let sorted = RecordBatch::concat(&all_batches)?.sort(
                        &params.order_by,
                        &params.descending,
                        &params.nulls_first,
                    )?;

                    let new_cols: Vec<Series> = params
                        .window_exprs
                        .iter()
                        .zip(&params.aliases)
                        .map(|(window_expr, name)| -> DaftResult<Series> {
                            match window_expr.as_ref() {
                                WindowExpr::RowNumber => sorted.window_row_number_col(name),
                                WindowExpr::Rank => {
                                    sorted.window_rank_col(name, &params.order_by, false)
                                }
                                WindowExpr::DenseRank => {
                                    sorted.window_rank_col(name, &params.order_by, true)
                                }
                                WindowExpr::CumeDist => {
                                    sorted.window_cume_dist_col(name, &params.order_by)
                                }
                                WindowExpr::PercentRank => {
                                    sorted.window_percent_rank_col(name, &params.order_by)
                                }
                                WindowExpr::Ntile(n) => sorted.window_ntile_col(name, *n),
                                _ => Err(DaftError::ValueError(
                                    format!(
                                        "Unsupported window function for order by only: {:?}",
                                        window_expr
                                    )
                                    .into(),
                                )),
                            }
                        })
                        .collect::<DaftResult<_>>()?;

                    let result = if new_cols.is_empty() {
                        sorted
                    } else {
                        sorted.union(&RecordBatch::from_nonempty_columns(new_cols)?)?
                    };

                    Ok(BlockingSinkOutput::Partitions(vec![
                        MicroPartition::new_loaded(
                            params.original_schema.clone(),
                            vec![result].into(),
                            None,
                        ),
                    ]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "WindowOrderByOnly".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Window
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

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(WindowOrderByOnlyState::new())
    }
}
