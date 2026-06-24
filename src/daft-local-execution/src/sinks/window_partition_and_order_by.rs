use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_core::prelude::*;
use daft_dsl::{
    Expr, WindowExpr,
    expr::bound_expr::{BoundExpr, BoundWindowExpr},
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::{
    blocking_sink::{
        BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    },
    window_base::{
        WindowBaseState, WindowSinkParams, finalize_partitioned_windows, partition_into_groups,
        sort_and_materialize_groups, window_spill_dirs,
    },
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    spill::SpillConfig,
};

struct WindowPartitionAndOrderByParams {
    window_exprs: Vec<BoundWindowExpr>,
    aliases: Vec<String>,
    partition_by: Vec<BoundExpr>,
    order_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    original_schema: SchemaRef,
}

impl WindowSinkParams for WindowPartitionAndOrderByParams {
    fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    fn partition_by(&self) -> &[BoundExpr] {
        &self.partition_by
    }

    fn name(&self) -> &'static str {
        "WindowPartitionAndOrderBy"
    }
}

pub struct WindowPartitionAndOrderBySink {
    window_partition_and_order_by_params: Arc<WindowPartitionAndOrderByParams>,
    spill_config: Option<SpillConfig>,
}

impl WindowPartitionAndOrderBySink {
    pub fn new(
        window_exprs: &[BoundWindowExpr],
        aliases: &[String],
        partition_by: &[BoundExpr],
        order_by: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
        schema: &SchemaRef,
        spill_config: Option<SpillConfig>,
    ) -> DaftResult<Self> {
        Ok(Self {
            window_partition_and_order_by_params: Arc::new(WindowPartitionAndOrderByParams {
                window_exprs: window_exprs.to_vec(),
                aliases: aliases.to_vec(),
                partition_by: partition_by.to_vec(),
                order_by: order_by.to_vec(),
                descending: descending.to_vec(),
                nulls_first: nulls_first.to_vec(),
                original_schema: schema.clone(),
            }),
            spill_config,
        })
    }

    fn num_partitions(&self) -> usize {
        self.max_concurrency()
    }
}

impl BlockingSink for WindowPartitionAndOrderBySink {
    type State = WindowBaseState;

    #[instrument(skip_all, name = "WindowPartitionAndOrderBySink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let params = self.window_partition_and_order_by_params.clone();
        let sink_name = params.name().to_string();
        spawner
            .spawn(
                async move {
                    state.push(input, params.partition_by(), &sink_name)?;
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "WindowPartitionAndOrderBySink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.window_partition_and_order_by_params.clone();
        let num_partitions = self.num_partitions();

        if params.partition_by.is_empty() {
            return Err(DaftError::ValueError(
                "Partition by cannot be empty for window functions".into(),
            ))
            .into();
        }

        let schema = params.original_schema.clone();
        let compute = move |all_partitions: Vec<RecordBatch>| -> DaftResult<RecordBatch> {
            let groups = partition_into_groups(&all_partitions, &params.partition_by)?;
            let full_data = RecordBatch::concat(&all_partitions)?;
            let partitions = sort_and_materialize_groups(
                groups,
                full_data,
                &params.order_by,
                &params.descending,
                &params.nulls_first,
            )?;

            if partitions.is_empty() {
                return Ok(RecordBatch::empty(Some(params.original_schema.clone())));
            }

            let grouped_results: Vec<RecordBatch> = partitions
                .into_iter()
                .map(|partition| -> DaftResult<RecordBatch> {
                    let new_cols: Vec<Series> = params
                        .window_exprs
                        .iter()
                        .zip(params.aliases.iter())
                        .map(|(window_expr, name)| -> DaftResult<Series> {
                            match window_expr.as_ref() {
                                WindowExpr::Agg(agg_expr) => {
                                    let agg = partition.eval_expression(
                                        &BoundExpr::new_unchecked(Arc::new(Expr::Agg(
                                            agg_expr.clone(),
                                        ))),
                                    )?;
                                    Ok(agg.broadcast(partition.len())?.rename(name))
                                }
                                WindowExpr::RowNumber => partition.window_row_number_col(name),
                                WindowExpr::Rank => {
                                    partition.window_rank_col(name, &params.order_by, false)
                                }
                                WindowExpr::DenseRank => {
                                    partition.window_rank_col(name, &params.order_by, true)
                                }
                                WindowExpr::Offset {
                                    input,
                                    offset,
                                    default,
                                } => partition.window_offset_col(
                                    name,
                                    BoundExpr::new_unchecked(input.clone()),
                                    *offset,
                                    default.clone().map(BoundExpr::new_unchecked),
                                ),
                                WindowExpr::FirstValue(_, _) | WindowExpr::LastValue(_, _) => {
                                    unreachable!("first_value/last_value require a frame and cannot appear in a partition+order_by-only window")
                                }
                            }
                        })
                        .collect::<DaftResult<_>>()?;

                    if new_cols.is_empty() {
                        Ok(partition)
                    } else {
                        partition.union(&RecordBatch::from_nonempty_columns(new_cols)?)
                    }
                })
                .collect::<DaftResult<_>>()?;

            RecordBatch::concat(&grouped_results)
        };

        spawner
            .spawn(
                finalize_partitioned_windows(states, num_partitions, schema, compute),
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "WindowPartitionAndOrderBy".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Window
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "WindowPartitionAndOrderBy: {}",
            self.window_partition_and_order_by_params
                .window_exprs
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        display.push(format!(
            "Partition by: {}",
            self.window_partition_and_order_by_params
                .partition_by
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        display.push(format!(
            "Order by: {}",
            self.window_partition_and_order_by_params
                .order_by
                .iter()
                .zip(self.window_partition_and_order_by_params.descending.iter())
                .zip(self.window_partition_and_order_by_params.nulls_first.iter())
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
        WindowBaseState::make_base_state(
            self.num_partitions(),
            window_spill_dirs(&self.spill_config),
            self.spill_config.as_ref().and_then(|sc| sc.cap()),
        )
    }
}
