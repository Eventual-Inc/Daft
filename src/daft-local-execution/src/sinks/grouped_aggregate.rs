use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use common_daft_config::DaftExecutionConfig;
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

#[derive(Clone)]
enum AggStrategy {
    // TODO: This would probably benefit from doing sharded aggs.
    AggThenPartition,
    PartitionThenAgg(usize),
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
            Self::PartitionThenAgg(threshold) => {
                Self::execute_partition_then_agg(inner_states, input, params, *threshold)
            }
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
            let state = state.get_or_insert_default();
            state.partially_aggregated.push(p);
        }
        Ok(())
    }

    fn execute_partition_then_agg(
        inner_states: &mut [Option<SinglePartitionAggregateState>],
        input: Arc<MicroPartition>,
        params: &GroupedAggregateParams,
        partial_agg_threshold: usize,
    ) -> DaftResult<()> {
        let partitioned =
            input.partition_by_hash(params.group_by.as_slice(), inner_states.len())?;
        for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
            let state = state.get_or_insert_default();
            if state.unaggregated_size + p.len() >= partial_agg_threshold {
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
            let state = state.get_or_insert_default();
            state.unaggregated_size += p.len();
            state.unaggregated.push(p);
        }
        Ok(())
    }
}

#[derive(Default)]
struct SinglePartitionAggregateState {
    partially_aggregated: Vec<MicroPartition>,
    unaggregated: Vec<MicroPartition>,
    unaggregated_size: usize,
}

enum GroupedAggregateState {
    Accumulating {
        inner_states: Vec<Option<SinglePartitionAggregateState>>,
        strategy: Option<AggStrategy>,
        partial_agg_threshold: usize,
        high_cardinality_threshold_ratio: f64,
    },
    Done,
}

impl GroupedAggregateState {
    fn new(
        num_partitions: usize,
        partial_agg_threshold: usize,
        high_cardinality_threshold_ratio: f64,
    ) -> Self {
        let inner_states = (0..num_partitions).map(|_| None).collect::<Vec<_>>();
        Self::Accumulating {
            inner_states,
            strategy: None,
            partial_agg_threshold,
            high_cardinality_threshold_ratio,
        }
    }

    fn push(
        &mut self,
        input: Arc<MicroPartition>,
        params: &GroupedAggregateParams,
        global_strategy_lock: &Arc<Mutex<Option<AggStrategy>>>,
    ) -> DaftResult<()> {
        let Self::Accumulating {
            ref mut inner_states,
            strategy,
            partial_agg_threshold,
            high_cardinality_threshold_ratio,
        } = self
        else {
            panic!("GroupedAggregateSink should be in Accumulating state");
        };

        // If we have determined a strategy, execute it.
        if let Some(strategy) = strategy {
            strategy.execute_strategy(inner_states, input, params)?;
        } else {
            // Otherwise, determine the strategy and execute
            let decided_strategy = Self::determine_agg_strategy(
                &input,
                params,
                *high_cardinality_threshold_ratio,
                *partial_agg_threshold,
                strategy,
                global_strategy_lock,
            )?;
            decided_strategy.execute_strategy(inner_states, input, params)?;
        }
        Ok(())
    }

    fn determine_agg_strategy(
        input: &Arc<MicroPartition>,
        params: &GroupedAggregateParams,
        high_cardinality_threshold_ratio: f64,
        partial_agg_threshold: usize,
        local_strategy_cache: &mut Option<AggStrategy>,
        global_strategy_lock: &Arc<Mutex<Option<AggStrategy>>>,
    ) -> DaftResult<AggStrategy> {
        let mut global_strategy = global_strategy_lock.lock().unwrap();
        // If some other worker has determined a strategy, use that.
        if let Some(global_strat) = global_strategy.as_ref() {
            *local_strategy_cache = Some(global_strat.clone());
            return Ok(global_strat.clone());
        }

        // Else determine the strategy.
        let groupby = input.eval_expression_list(params.group_by.as_slice())?;

        let groupkey_hashes = groupby
            .get_tables()?
            .iter()
            .map(|t| t.hash_rows())
            .collect::<DaftResult<Vec<_>>>()?;
        let estimated_num_groups = groupkey_hashes
            .iter()
            .flatten()
            .collect::<HashSet<_>>()
            .len();

        let decided_strategy = if estimated_num_groups as f64 / input.len() as f64
            >= high_cardinality_threshold_ratio
        {
            AggStrategy::PartitionThenAgg(partial_agg_threshold)
        } else {
            AggStrategy::AggThenPartition
        };

        *local_strategy_cache = Some(decided_strategy.clone());
        *global_strategy = Some(decided_strategy.clone());
        Ok(decided_strategy)
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
    partial_agg_threshold: usize,
    high_cardinality_threshold_ratio: f64,
    global_strategy_lock: Arc<Mutex<Option<AggStrategy>>>,
}

impl GroupedAggregateSink {
    pub fn new(
        aggregations: &[ExprRef],
        group_by: &[ExprRef],
        schema: &SchemaRef,
        cfg: &DaftExecutionConfig,
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
            .collect::<Vec<_>>();
        let final_group_by = if !partial_agg_exprs.is_empty() {
            group_by.iter().map(|e| col(e.name())).collect::<Vec<_>>()
        } else {
            group_by.to_vec()
        };
        let strategy = if partial_agg_exprs.is_empty() && !final_agg_exprs.is_empty() {
            Some(AggStrategy::PartitionOnly)
        } else {
            None
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
            partial_agg_threshold: cfg.partial_aggregation_threshold,
            high_cardinality_threshold_ratio: cfg.high_cardinality_aggregation_threshold,
            global_strategy_lock: Arc::new(Mutex::new(strategy)),
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
        let strategy_lock = self.global_strategy_lock.clone();
        runtime
            .spawn(
                async move {
                    let agg_state = state
                        .as_any_mut()
                        .downcast_mut::<GroupedAggregateState>()
                        .expect("GroupedAggregateSink should have GroupedAggregateState");

                    agg_state.push(input, &params, &strategy_lock)?;
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
        Ok(Box::new(GroupedAggregateState::new(
            self.num_partitions(),
            self.partial_agg_threshold,
            self.high_cardinality_threshold_ratio,
        )))
    }
}
