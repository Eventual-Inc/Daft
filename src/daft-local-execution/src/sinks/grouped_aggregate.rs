use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::{
    bound_col,
    bound_expr::{BoundAggExpr, BoundExpr},
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::{InlineAggState, can_inline_agg_with_schema};
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
};

#[derive(Clone, Debug)]
pub(crate) enum AggStrategy {
    // TODO: This would probably benefit from doing sharded aggs.
    AggThenPartition,
    PartitionThenAgg(usize),
    PartitionOnly,
    /// Persistent hash table per worker — avoids per-morsel hash table rebuild.
    InlineRecycle,
}

impl AggStrategy {
    fn execute_strategy(
        &self,
        inner_states: &mut [Option<SinglePartitionAggregateState>],
        input: MicroPartition,
        params: &GroupedAggregateParams,
        inline_state: &mut Option<InlineAggState>,
    ) -> DaftResult<()> {
        match self {
            Self::AggThenPartition => Self::execute_agg_then_partition(inner_states, input, params),
            Self::PartitionThenAgg(threshold) => {
                Self::execute_partition_then_agg(inner_states, input, params, *threshold)
            }
            Self::PartitionOnly => Self::execute_partition_only(inner_states, input, params),
            Self::InlineRecycle => Self::execute_inline_recycle(inline_state, input, params),
        }
    }

    fn execute_agg_then_partition(
        inner_states: &mut [Option<SinglePartitionAggregateState>],
        input: MicroPartition,
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
        input: MicroPartition,
        params: &GroupedAggregateParams,
        partial_agg_threshold: usize,
    ) -> DaftResult<()> {
        let partitioned =
            input.partition_by_hash(params.group_by.as_slice(), inner_states.len())?;
        for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
            let state = state.get_or_insert_default();
            if state.unaggregated_size + p.len() >= partial_agg_threshold {
                let mut unaggregated = std::mem::take(&mut state.unaggregated);
                unaggregated.push(p);
                let aggregated = MicroPartition::concat(unaggregated)?.agg(
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
        input: MicroPartition,
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

    fn execute_inline_recycle(
        inline_state: &mut Option<InlineAggState>,
        input: MicroPartition,
        params: &GroupedAggregateParams,
    ) -> DaftResult<()> {
        let state = inline_state.get_or_insert_with(|| {
            InlineAggState::try_new(&params.partial_agg_exprs, &params.group_by, &input.schema())
                .expect("InlineRecycle strategy should only be used when InlineAggState is valid")
                .expect("InlineRecycle strategy should only be used when InlineAggState is valid")
        });
        for rb in input.record_batches() {
            state.push_batch(rb)?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct SinglePartitionAggregateState {
    partially_aggregated: Vec<MicroPartition>,
    unaggregated: Vec<MicroPartition>,
    unaggregated_size: usize,
}

pub(crate) enum GroupedAggregateState {
    Accumulating {
        inner_states: Vec<Option<SinglePartitionAggregateState>>,
        inline_state: Option<InlineAggState>,
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
            inline_state: None,
            strategy: None,
            partial_agg_threshold,
            high_cardinality_threshold_ratio,
        }
    }

    fn push(
        &mut self,
        input: MicroPartition,
        params: &GroupedAggregateParams,
        global_strategy_lock: &Arc<Mutex<Option<AggStrategy>>>,
    ) -> DaftResult<()> {
        let Self::Accumulating {
            inner_states,
            inline_state,
            strategy,
            partial_agg_threshold,
            high_cardinality_threshold_ratio,
        } = self
        else {
            panic!("GroupedAggregateSink should be in Accumulating state");
        };

        // If we have determined a strategy, execute it.
        if let Some(strategy) = strategy {
            strategy.execute_strategy(inner_states, input, params, inline_state)?;
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
            decided_strategy.execute_strategy(inner_states, input, params, inline_state)?;
        }
        Ok(())
    }

    fn determine_agg_strategy(
        input: &MicroPartition,
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
            .record_batches()
            .iter()
            .map(|t| t.hash_rows())
            .collect::<DaftResult<Vec<_>>>()?;
        let estimated_num_groups = groupkey_hashes
            .iter()
            .flatten()
            .collect::<HashSet<_>>()
            .len();

        let is_high_cardinality =
            estimated_num_groups as f64 / input.len() as f64 >= high_cardinality_threshold_ratio;

        let decided_strategy = if is_high_cardinality {
            AggStrategy::PartitionThenAgg(partial_agg_threshold)
        } else if can_inline_agg_with_schema(&params.partial_agg_exprs, &input.schema()) {
            AggStrategy::InlineRecycle
        } else {
            AggStrategy::AggThenPartition
        };

        *local_strategy_cache = Some(decided_strategy.clone());
        *global_strategy = Some(decided_strategy.clone());
        Ok(decided_strategy)
    }

    fn finalize(
        &mut self,
    ) -> (
        Vec<Option<SinglePartitionAggregateState>>,
        Option<InlineAggState>,
    ) {
        let (inner_states, inline_state) = if let Self::Accumulating {
            inner_states,
            inline_state,
            ..
        } = self
        {
            (std::mem::take(inner_states), inline_state.take())
        } else {
            panic!("GroupedAggregateSink should be in Accumulating state");
        };
        *self = Self::Done;
        (inner_states, inline_state)
    }
}

struct GroupedAggregateParams {
    // The original aggregations and group by expressions
    original_aggregations: Vec<BoundAggExpr>,
    group_by: Vec<BoundExpr>,
    // The expressions for to be used for partial aggregation
    partial_agg_exprs: Vec<BoundAggExpr>,
    // The expressions for the final aggregation
    final_agg_exprs: Vec<BoundAggExpr>,
    final_group_by: Vec<BoundExpr>,
    final_projections: Vec<BoundExpr>,
}

pub struct GroupedAggregateSink {
    grouped_aggregate_params: Arc<GroupedAggregateParams>,
    partial_agg_threshold: usize,
    high_cardinality_threshold_ratio: f64,
    global_strategy_lock: Arc<Mutex<Option<AggStrategy>>>,
}

impl GroupedAggregateSink {
    pub fn new(
        aggregations: &[BoundAggExpr],
        group_by: &[BoundExpr],
        input_schema: &SchemaRef,
        cfg: &DaftExecutionConfig,
    ) -> DaftResult<Self> {
        let (partial_agg_exprs, final_agg_exprs, final_projections) =
            daft_local_plan::agg::populate_aggregation_stages_bound(
                aggregations,
                input_schema,
                group_by,
            )?;

        // MapGroups aggregations cannot be decomposed into partial / final stages and
        // must see the full group in a single pass. Detect this case so that we force
        // a partition-only strategy and run the original aggregations during the
        // final aggregation step.
        let has_map_groups = aggregations
            .iter()
            .any(|agg| matches!(agg.as_ref(), daft_dsl::AggExpr::MapGroups { .. }));

        let final_group_by = if !partial_agg_exprs.is_empty() {
            group_by
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    let field = e.as_ref().to_field(input_schema)?;
                    Ok(BoundExpr::new_unchecked(bound_col(i, field)))
                })
                .collect::<DaftResult<Vec<_>>>()?
        } else {
            group_by.to_vec()
        };

        let strategy = if has_map_groups {
            // Always use partition-only for MapGroups so that we only hash-partition
            // the data by the group keys and then run the original MapGroups
            // aggregation once per partition in `finalize`.
            Some(AggStrategy::PartitionOnly)
        } else if partial_agg_exprs.is_empty() && !final_agg_exprs.is_empty() {
            Some(AggStrategy::PartitionOnly)
        } else {
            None
        };

        Ok(Self {
            grouped_aggregate_params: Arc::new(GroupedAggregateParams {
                original_aggregations: aggregations.to_vec(),
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
        self.max_concurrency()
    }
}

impl BlockingSink for GroupedAggregateSink {
    type State = GroupedAggregateState;
    #[instrument(skip_all, name = "GroupedAggregateSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let params = self.grouped_aggregate_params.clone();
        let strategy_lock = self.global_strategy_lock.clone();
        spawner
            .spawn(
                async move {
                    state.push(input, &params, &strategy_lock)?;
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "GroupedAggregateSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.grouped_aggregate_params.clone();
        let num_partitions = self.num_partitions();
        spawner
            .spawn(
                async move {
                    // Collect inline states and partition-based states separately.
                    let mut inline_states: Vec<InlineAggState> = Vec::new();
                    let mut state_iters: Vec<
                        std::vec::IntoIter<Option<SinglePartitionAggregateState>>,
                    > = Vec::new();

                    for mut state in states {
                        let (inner_states, inline_state) = state.finalize();
                        if let Some(is) = inline_state {
                            inline_states.push(is);
                        }
                        state_iters.push(inner_states.into_iter());
                    }

                    // Materialize inline partial results and partition them.
                    let mut inline_per_partition: Vec<Vec<MicroPartition>> =
                        (0..num_partitions).map(|_| Vec::new()).collect();
                    for is in inline_states {
                        if is.num_groups() > 0 {
                            let materialized = is.finalize()?;
                            let schema = materialized.schema.clone();
                            let mp = MicroPartition::new_loaded(
                                schema,
                                Arc::new(vec![materialized]),
                                None,
                            );
                            let partitioned = mp.partition_by_hash(
                                params.final_group_by.as_slice(),
                                num_partitions,
                            )?;
                            for (i, p) in partitioned.into_iter().enumerate() {
                                if p.len() > 0 {
                                    inline_per_partition[i].push(p);
                                }
                            }
                        }
                    }

                    let mut per_partition_finalize_tasks = tokio::task::JoinSet::new();
                    for part_idx in 0..num_partitions {
                        let per_partition_state: Vec<Option<SinglePartitionAggregateState>> =
                            state_iters
                                .iter_mut()
                                .map(|state| {
                                    state.next().expect(
                                "GroupedAggregateState should have SinglePartitionAggregateState",
                            )
                                })
                                .collect();
                        let params = params.clone();
                        let inline_parts = std::mem::take(&mut inline_per_partition[part_idx]);
                        per_partition_finalize_tasks.spawn(async move {
                            let mut unaggregated = vec![];
                            let mut partially_aggregated = vec![];
                            for state in per_partition_state.into_iter().flatten() {
                                unaggregated.extend(state.unaggregated);
                                partially_aggregated.extend(state.partially_aggregated);
                            }
                            // Inline partial results are already partially aggregated.
                            partially_aggregated.extend(inline_parts);

                            // If we have no partially aggregated partitions, aggregate the unaggregated partitions using the original aggregations
                            if params.partial_agg_exprs.is_empty() && !unaggregated.is_empty() {
                                let concated = MicroPartition::concat(unaggregated)?;
                                let agged = concated
                                    .agg(&params.original_aggregations, &params.group_by)?;
                                Ok(agged)
                            }
                            // If we have no unaggregated partitions, finalize the partially aggregated partitions
                            else if unaggregated.is_empty() {
                                if partially_aggregated.is_empty() {
                                    return Ok(MicroPartition::empty(None));
                                }
                                let concated = MicroPartition::concat(partially_aggregated)?;
                                let agged = concated
                                    .agg(&params.final_agg_exprs, &params.final_group_by)?;
                                let projected =
                                    agged.eval_expression_list(&params.final_projections)?;
                                Ok(projected)
                            }
                            // Otherwise, partially aggregate the unaggregated partitions, concatenate them with the partially aggregated partitions, and finalize the result.
                            else {
                                let leftover_partial_agg = MicroPartition::concat(unaggregated)?
                                    .agg(&params.partial_agg_exprs, &params.group_by)?;
                                partially_aggregated.push(leftover_partial_agg);
                                let concated = MicroPartition::concat(partially_aggregated)?;
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
                    Ok(BlockingSinkOutput::Partitions(results))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "GroupedAggregate".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::GroupByAgg
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "GroupedAggregate: {}",
            self.grouped_aggregate_params
                .original_aggregations
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        display.push(format!(
            "Group by: {}",
            self.grouped_aggregate_params
                .group_by
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        display
    }

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(GroupedAggregateState::new(
            self.num_partitions(),
            self.partial_agg_threshold,
            self.high_cardinality_threshold_ratio,
        ))
    }
}
