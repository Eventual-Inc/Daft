use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_core::prelude::SchemaRef;
use daft_dsl::{col, Expr, ExprRef};
use daft_micropartition::MicroPartition;
use daft_physical_plan::extract_agg_expr;
use tracing::{info_span, instrument, Instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::NUM_CPUS;

enum AggStrategy {
    // TODO: This would probably benefit from doing sharded aggs.
    AggThenPartition,
    PartitionThenAgg,
    PartitionOnly,
}

impl AggStrategy {
    fn execute_strategy(
        &self,
        inner_states: &mut [Option<SinglePartitionAggregateState>],
        input: Arc<MicroPartition>,
        params: &GroupedAggregateParams,
    ) -> DaftResult<()> {
        match self {
            Self::AggThenPartition => Self::execute_agg_then_partition(inner_states, input, params),
            Self::PartitionThenAgg => Self::execute_partition_then_agg(inner_states, input, params),
            Self::PartitionOnly => Self::execute_partition_only(inner_states, input, params),
        }
    }

    fn execute_agg_then_partition(
        inner_states: &mut [Option<SinglePartitionAggregateState>],
        input: Arc<MicroPartition>,
        params: &GroupedAggregateParams,
    ) -> DaftResult<()> {
        let agged = input.agg(
            params.partial_agg_exprs.as_slice(),
            params.group_by.as_slice(),
        )?;
        let partitioned =
            agged.partition_by_hash(params.final_group_by.as_slice(), inner_states.len())?;
        for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
            let state = state.get_or_insert_with(|| SinglePartitionAggregateState {
                partially_aggregated: vec![],
                unaggregated: vec![],
                unaggregated_size: 0,
            });
            state.partially_aggregated.push(p);
        }
        Ok(())
    }

    fn execute_partition_then_agg(
        inner_states: &mut [Option<SinglePartitionAggregateState>],
        input: Arc<MicroPartition>,
        params: &GroupedAggregateParams,
    ) -> DaftResult<()> {
        let partitioned =
            input.partition_by_hash(params.group_by.as_slice(), inner_states.len())?;
        for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
            let state = state.get_or_insert_with(|| SinglePartitionAggregateState {
                partially_aggregated: vec![],
                unaggregated: vec![],
                unaggregated_size: 0,
            });
            if state.unaggregated_size + p.len() >= GroupedAggregateState::PARTIAL_AGG_THRESHOLD {
                let unaggregated = std::mem::take(&mut state.unaggregated);
                let aggregated =
                    MicroPartition::concat(unaggregated.iter().chain(std::iter::once(&p)))?.agg(
                        params.partial_agg_exprs.as_slice(),
                        params.group_by.as_slice(),
                    )?;
                state.partially_aggregated.push(aggregated);
                state.unaggregated_size = 0;
            } else {
                state.unaggregated_size += p.len();
                state.unaggregated.push(p);
            }
        }
        Ok(())
    }

    fn execute_partition_only(
        inner_states: &mut [Option<SinglePartitionAggregateState>],
        input: Arc<MicroPartition>,
        params: &GroupedAggregateParams,
    ) -> DaftResult<()> {
        let partitioned =
            input.partition_by_hash(params.group_by.as_slice(), inner_states.len())?;
        for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
            let state = state.get_or_insert_with(|| SinglePartitionAggregateState {
                partially_aggregated: vec![],
                unaggregated: vec![],
                unaggregated_size: 0,
            });
            state.unaggregated_size += p.len();
            state.unaggregated.push(p);
        }
        Ok(())
    }
}

struct SinglePartitionAggregateState {
    partially_aggregated: Vec<MicroPartition>,
    unaggregated: Vec<MicroPartition>,
    unaggregated_size: usize,
}

enum GroupedAggregateState {
    Accumulating {
        inner_states: Vec<Option<SinglePartitionAggregateState>>,
        strategy: Option<AggStrategy>,
    },
    Done,
}

impl GroupedAggregateState {
    // This is the threshold for when we should aggregate any accumulated unaggregated partitions
    const PARTIAL_AGG_THRESHOLD: usize = 10_000;
    // This is the threshold that indicate high cardinality grouping in the input data, and we should use partitioning first strategy.
    // It is the ratio between the number of groups vs the number of rows.
    // For example if the input data had 100 rows and 80 groups or above, that's considered high cardinality.
    const HIGH_CARDINALITY_THRESHOLD_RATIO: f64 = 0.8;

    fn new(num_partitions: usize) -> Self {
        let inner_states = (0..num_partitions).map(|_| None).collect();
        Self::Accumulating {
            inner_states,
            strategy: None,
        }
    }

    fn push(
        &mut self,
        input: Arc<MicroPartition>,
        params: &GroupedAggregateParams,
    ) -> DaftResult<()> {
        if let Self::Accumulating {
            ref mut inner_states,
            strategy,
        } = self
        {
            // If we have determined a strategy, execute it.
            if let Some(strategy) = strategy {
                strategy.execute_strategy(inner_states, input, params)?;
            }
            // Otherwise, determine a strategy based on the input data.
            else {
                // If we have no partial aggregation expressions and only final aggregation expressions, e.g. map_groups, we don't need to partially aggregate
                // and can just partition the input data.
                if params.partial_agg_exprs.is_empty() && !params.final_agg_exprs.is_empty() {
                    *strategy = Some(AggStrategy::PartitionOnly);
                    strategy
                        .as_ref()
                        .unwrap()
                        .execute_strategy(inner_states, input, params)?;
                    return Ok(());
                }
                // If we do have partial aggregation expressions, we need to estimate the cardinality of the input data
                // to determine the best strategy. If the cardinality is high, we should do partition first, otherwise we should do aggregation first.
                let agged = input.agg(
                    params.partial_agg_exprs.as_slice(),
                    params.group_by.as_slice(),
                )?;
                let partitioned = agged
                    .partition_by_hash(params.final_group_by.as_slice(), inner_states.len())?;
                for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
                    let state = state.get_or_insert_with(|| SinglePartitionAggregateState {
                        partially_aggregated: vec![],
                        unaggregated: vec![],
                        unaggregated_size: 0,
                    });
                    state.partially_aggregated.push(p);
                }
                if agged.len() as f64 / input.len() as f64 >= Self::HIGH_CARDINALITY_THRESHOLD_RATIO
                {
                    *strategy = Some(AggStrategy::PartitionThenAgg);
                } else {
                    *strategy = Some(AggStrategy::AggThenPartition);
                }
            }
        } else {
            panic!("GroupedAggregateSink should be in Accumulating state");
        }
        Ok(())
    }

    fn finalize(&mut self) -> Vec<Option<SinglePartitionAggregateState>> {
        let res = if let Self::Accumulating {
            ref mut inner_states,
            ..
        } = self
        {
            std::mem::take(inner_states)
        } else {
            panic!("GroupedAggregateSink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for GroupedAggregateState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct GroupedAggregateParams {
    // The original aggregations and group by expressions
    original_aggregations: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
    // The expressions for to be used for partial aggregation
    partial_agg_exprs: Vec<ExprRef>,
    // The expressions for the final aggregation
    final_agg_exprs: Vec<ExprRef>,
    final_group_by: Vec<ExprRef>,
    final_projections: Vec<ExprRef>,
}

pub struct GroupedAggregateSink {
    grouped_aggregate_params: Arc<GroupedAggregateParams>,
}

impl GroupedAggregateSink {
    pub fn new(
        aggregations: &[ExprRef],
        group_by: &[ExprRef],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        let aggregations = aggregations
            .iter()
            .map(extract_agg_expr)
            .collect::<DaftResult<Vec<_>>>()?;
        let (partial_aggs, final_aggs, final_projections) =
            daft_physical_plan::populate_aggregation_stages(&aggregations, schema, group_by);
        let partial_agg_exprs = partial_aggs
            .into_values()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect::<Vec<_>>();
        let final_agg_exprs = final_aggs
            .into_values()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect();
        let final_group_by = if !partial_agg_exprs.is_empty() {
            group_by.iter().map(|e| col(e.name())).collect::<Vec<_>>()
        } else {
            group_by.to_vec()
        };

        Ok(Self {
            grouped_aggregate_params: Arc::new(GroupedAggregateParams {
                original_aggregations: aggregations
                    .into_iter()
                    .map(|e| Expr::Agg(e).into())
                    .collect(),
                group_by: group_by.to_vec(),
                partial_agg_exprs,
                final_agg_exprs,
                final_group_by,
                final_projections,
            }),
        })
    }

    fn num_partitions(&self) -> usize {
        *NUM_CPUS
    }
}

impl BlockingSink for GroupedAggregateSink {
    #[instrument(skip_all, name = "GroupedAggregateSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        runtime: &RuntimeRef,
    ) -> BlockingSinkSinkResult {
        let params = self.grouped_aggregate_params.clone();
        runtime
            .spawn(
                async move {
                    let agg_state = state
                        .as_any_mut()
                        .downcast_mut::<GroupedAggregateState>()
                        .expect("GroupedAggregateSink should have GroupedAggregateState");

                    agg_state.push(input, &params)?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                }
                .instrument(info_span!("GroupedAggregateSink::sink")),
            )
            .into()
    }

    #[instrument(skip_all, name = "GroupedAggregateSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        runtime: &RuntimeRef,
    ) -> BlockingSinkFinalizeResult {
        let params = self.grouped_aggregate_params.clone();
        let num_partitions = self.num_partitions();
        runtime
            .spawn(
                async move {
                    let mut state_iters = states
                        .into_iter()
                        .map(|mut state| {
                            state
                                .as_any_mut()
                                .downcast_mut::<GroupedAggregateState>()
                                .expect("GroupedAggregateSink should have GroupedAggregateState")
                                .finalize()
                                .into_iter()
                        })
                        .collect::<Vec<_>>();

                    let mut per_partition_finalize_tasks = tokio::task::JoinSet::new();
                    for _ in 0..num_partitions {
                        let per_partition_state = state_iters
                            .iter_mut()
                            .map(|state| {
                                state.next().expect(
                                "GroupedAggregateState should have SinglePartitionAggregateState",
                            )
                            })
                            .collect::<Vec<_>>();
                        let params = params.clone();
                        per_partition_finalize_tasks.spawn(async move {
                            let mut unaggregated = vec![];
                            let mut partially_aggregated = vec![];
                            for state in per_partition_state.into_iter().flatten() {
                                unaggregated.extend(state.unaggregated);
                                partially_aggregated.extend(state.partially_aggregated);
                            }

                            // If we have no partially aggregated partitions, aggregate the unaggregated partitions using the original aggregations
                            if partially_aggregated.is_empty() {
                                let concated = MicroPartition::concat(&unaggregated)?;
                                let agged = concated
                                    .agg(&params.original_aggregations, &params.group_by)?;
                                Ok(agged)
                            }
                            // If we have no unaggregated partitions, finalize the partially aggregated partitions
                            else if unaggregated.is_empty() {
                                let concated = MicroPartition::concat(&partially_aggregated)?;
                                let agged = concated
                                    .agg(&params.final_agg_exprs, &params.final_group_by)?;
                                let projected =
                                    agged.eval_expression_list(&params.final_projections)?;
                                Ok(projected)
                            }
                            // Otherwise, partially aggregate the unaggregated partitions, concatenate them with the partially aggregated partitions, and finalize the result.
                            else {
                                let leftover_partial_agg =
                                    MicroPartition::concat(&unaggregated)?
                                        .agg(&params.partial_agg_exprs, &params.group_by)?;
                                let concated = MicroPartition::concat(
                                    partially_aggregated
                                        .iter()
                                        .chain(std::iter::once(&leftover_partial_agg)),
                                )?;
                                let agged = concated
                                    .agg(&params.final_agg_exprs, &params.final_group_by)?;
                                let projected =
                                    agged.eval_expression_list(&params.final_projections)?;
                                Ok(projected)
                            }
                        });
                    }
                    let results = per_partition_finalize_tasks
                        .join_all()
                        .await
                        .into_iter()
                        .collect::<DaftResult<Vec<_>>>()?;
                    let concated = MicroPartition::concat(&results)?;
                    Ok(Some(Arc::new(concated)))
                }
                .instrument(info_span!("GroupedAggregateSink::finalize")),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "GroupedAggregateSink"
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(GroupedAggregateState::new(self.num_partitions())))
    }
}
