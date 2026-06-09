use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::{
    bound_col,
    bound_expr::{BoundAggExpr, BoundExpr},
};
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    spill::{SpillConfig, SpillStore, SpillWriter},
};

/// Cap on recursive sub-partitioning depth when a single hash bucket still exceeds the memory
/// budget at finalize (many distinct groups colliding into one bucket).
const MAX_AGG_RECURSION: u64 = 4;

#[derive(Clone, Debug)]
pub(crate) enum AggStrategy {
    // TODO: This would probably benefit from doing sharded aggs.
    AggThenPartition,
    PartitionThenAgg(usize),
    PartitionOnly,
}

impl AggStrategy {
    fn execute_strategy(
        &self,
        inner_states: &mut [Option<SinglePartitionAggregateState>],
        input: MicroPartition,
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
            let bytes = p.size_bytes();
            state.partially_aggregated.push(p);
            state.partial_bytes += bytes;
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
                state.partial_bytes += aggregated.size_bytes();
                state.partially_aggregated.push(aggregated);
                state.unaggregated_size = 0;
                state.unagg_bytes = 0;
            } else {
                state.unaggregated_size += p.len();
                state.unagg_bytes += p.size_bytes();
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
            state.unagg_bytes += p.size_bytes();
            state.unaggregated.push(p);
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct SinglePartitionAggregateState {
    partially_aggregated: Vec<MicroPartition>,
    unaggregated: Vec<MicroPartition>,
    unaggregated_size: usize,
    /// Resident bytes of `partially_aggregated` / `unaggregated`, used to decide when to spill.
    partial_bytes: usize,
    unagg_bytes: usize,
}

pub(crate) enum GroupedAggregateState {
    Accumulating {
        inner_states: Vec<Option<SinglePartitionAggregateState>>,
        strategy: Option<AggStrategy>,
        partial_agg_threshold: usize,
        high_cardinality_threshold_ratio: f64,
        /// Spill destinations; `None` disables spilling for this sink.
        spill_dirs: Option<Vec<String>>,
        /// Per-hash-bucket in-memory budget before spilling.
        budget_per_bucket: usize,
        /// Lazily created on first spill (one IPC file per hash bucket).
        spill_writer: Option<SpillWriter>,
    },
}

impl GroupedAggregateState {
    fn new(
        num_partitions: usize,
        partial_agg_threshold: usize,
        high_cardinality_threshold_ratio: f64,
        spill_dirs: Option<Vec<String>>,
        budget_per_bucket: usize,
    ) -> Self {
        let inner_states = (0..num_partitions).map(|_| None).collect::<Vec<_>>();
        Self::Accumulating {
            inner_states,
            strategy: None,
            partial_agg_threshold,
            high_cardinality_threshold_ratio,
            spill_dirs,
            budget_per_bucket,
            spill_writer: None,
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
            strategy,
            partial_agg_threshold,
            high_cardinality_threshold_ratio,
            spill_dirs,
            budget_per_bucket,
            spill_writer,
        } = self;

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

        // Spill any bucket that has grown past the per-bucket budget.
        if let Some(dirs) = spill_dirs.as_ref() {
            Self::maybe_spill(
                inner_states,
                spill_writer,
                dirs,
                *budget_per_bucket,
                params,
            )?;
        }
        Ok(())
    }

    /// Spill (and reset) any hash bucket whose resident bytes exceed `budget`. For decomposable
    /// aggregations the bucket is first compacted to one partial-aggregate row per group (so the
    /// spilled bytes are small); for non-decomposable aggregations (`MapGroups`/`AggFn`) the raw
    /// partitioned rows are spilled as-is.
    fn maybe_spill(
        inner_states: &mut [Option<SinglePartitionAggregateState>],
        spill_writer: &mut Option<SpillWriter>,
        spill_dirs: &[String],
        budget: usize,
        params: &GroupedAggregateParams,
    ) -> DaftResult<()> {
        let decomposable = !params.partial_agg_exprs.is_empty();
        let num_buckets = inner_states.len();
        for p in 0..num_buckets {
            let Some(st) = inner_states[p].as_mut() else {
                continue;
            };
            if st.partial_bytes + st.unagg_bytes <= budget {
                continue;
            }

            let to_spill: MicroPartition = if decomposable {
                let mut partials = std::mem::take(&mut st.partially_aggregated);
                let unagg = std::mem::take(&mut st.unaggregated);
                st.unaggregated_size = 0;
                st.partial_bytes = 0;
                st.unagg_bytes = 0;
                if !unagg.is_empty() {
                    partials.push(
                        MicroPartition::concat(unagg)?
                            .agg(&params.partial_agg_exprs, &params.group_by)?,
                    );
                }
                if partials.is_empty() {
                    continue;
                }
                // Combine to one partial row per group via the associative combine step.
                MicroPartition::concat(partials)?
                    .agg(&params.final_agg_exprs, &params.final_group_by)?
            } else {
                let unagg = std::mem::take(&mut st.unaggregated);
                st.unaggregated_size = 0;
                st.unagg_bytes = 0;
                if unagg.is_empty() {
                    continue;
                }
                MicroPartition::concat(unagg)?
            };

            if to_spill.len() == 0 {
                continue;
            }

            let writer = match spill_writer {
                Some(w) => w,
                None => {
                    let schema = to_spill.schema();
                    let w =
                        SpillWriter::new(num_buckets, &schema, spill_dirs.to_vec(), "daft_agg_spill_")?;
                    *spill_writer = Some(w);
                    spill_writer.as_mut().unwrap()
                }
            };
            for rb in to_spill.record_batches() {
                writer.write_batch(p, rb)?;
            }
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

    /// Consume the state for finalize, yielding the in-memory per-bucket states and the spill writer
    /// (if any data was spilled).
    fn into_finalize_parts(
        self,
    ) -> (Vec<Option<SinglePartitionAggregateState>>, Option<SpillWriter>) {
        let Self::Accumulating {
            inner_states,
            spill_writer,
            ..
        } = self;
        (inner_states, spill_writer)
    }
}

struct GroupedAggregateParams {
    // Input schema, used to materialize a correctly-typed empty result when there are no rows.
    input_schema: SchemaRef,
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
    /// `Some` enables grace-aggregation spill-to-disk.
    spill_config: Option<SpillConfig>,
    /// Per-hash-bucket budget derived from the configured total spill budget and the number of
    /// hash buckets.
    budget_per_bucket: usize,
}

impl GroupedAggregateSink {
    pub fn new(
        aggregations: &[BoundAggExpr],
        group_by: &[BoundExpr],
        input_schema: &SchemaRef,
        cfg: &DaftExecutionConfig,
        spill_config: Option<SpillConfig>,
    ) -> DaftResult<Self> {
        let (partial_agg_exprs, final_agg_exprs, final_projections) =
            daft_local_plan::agg::populate_aggregation_stages_bound(
                aggregations,
                input_schema,
                group_by,
            )?;

        // MapGroups cannot be decomposed into partial/final stages — it must see the full
        // group in one pass, so it always uses PartitionOnly.  AggFn has no single-pass
        // path, so the two cannot coexist in the same aggregation.
        let has_map_groups = aggregations
            .iter()
            .any(|agg| matches!(agg.as_ref(), daft_dsl::AggExpr::MapGroups { .. }));
        let has_agg_fn = aggregations
            .iter()
            .any(|agg| matches!(agg.as_ref(), daft_dsl::AggExpr::AggFn { .. }));
        if has_map_groups && has_agg_fn {
            return Err(common_error::DaftError::ValueError(
                "Cannot mix MapGroups (Python UDFs) and extension aggregations (AggFn) \
                 in the same aggregation; split them into separate operations."
                    .to_string(),
            ));
        }

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

        // Split the total spill budget across all hash buckets (one bucket per concurrency slot,
        // per state, so `num_partitions^2` buckets total across the operator).
        let num_partitions = get_compute_pool_num_threads();
        let budget_per_bucket = spill_config
            .as_ref()
            .map(|sc| (sc.threshold_bytes / (num_partitions * num_partitions).max(1)).max(1))
            .unwrap_or(0);

        Ok(Self {
            grouped_aggregate_params: Arc::new(GroupedAggregateParams {
                input_schema: input_schema.clone(),
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
            spill_config,
            budget_per_bucket,
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
        // No spill => no recursion (unbounded budget keeps the original single-pass behavior).
        let recursion_budget = if self.spill_config.is_some() {
            self.budget_per_bucket
        } else {
            usize::MAX
        };
        spawner
            .spawn(
                async move {
                    // Consume each state: take its per-bucket in-memory data and seal its spill file.
                    let mut inners: Vec<Vec<Option<SinglePartitionAggregateState>>> =
                        Vec::with_capacity(states.len());
                    let mut stores: Vec<Option<SpillStore>> = Vec::with_capacity(states.len());
                    for state in states {
                        let (inner, writer) = state.into_finalize_parts();
                        inners.push(inner);
                        stores.push(match writer {
                            Some(w) => Some(w.finish()?),
                            None => None,
                        });
                    }
                    let stores = Arc::new(stores);

                    // Gather, for each hash bucket, that bucket's state across all input states.
                    let mut per_part: Vec<Vec<Option<SinglePartitionAggregateState>>> = (0
                        ..num_partitions)
                        .map(|p| {
                            inners
                                .iter_mut()
                                .map(|v| v.get_mut(p).and_then(Option::take))
                                .collect()
                        })
                        .collect();

                    // Finalize buckets with bounded concurrency to cap peak memory.
                    let max_inflight = get_compute_pool_num_threads().max(1);
                    let mut tasks: tokio::task::JoinSet<DaftResult<Option<MicroPartition>>> =
                        tokio::task::JoinSet::new();
                    let mut next = 0usize;

                    let mut spawn_next =
                        |tasks: &mut tokio::task::JoinSet<DaftResult<Option<MicroPartition>>>,
                         next: &mut usize| {
                            if *next >= num_partitions {
                                return;
                            }
                            let p = *next;
                            let parts = std::mem::take(&mut per_part[p]);
                            let params = params.clone();
                            let stores = stores.clone();
                            tasks.spawn(async move {
                                finalize_bucket(p, parts, stores, params, recursion_budget)
                            });
                            *next += 1;
                        };

                    while tasks.len() < max_inflight && next < num_partitions {
                        spawn_next(&mut tasks, &mut next);
                    }

                    let mut results = vec![];
                    while let Some(res) = tasks.join_next().await {
                        let bucket_result =
                            res.map_err(|e| DaftError::InternalError(e.to_string()))??;
                        if let Some(mp) = bucket_result
                            && mp.len() > 0
                        {
                            results.push(mp);
                        }
                        spawn_next(&mut tasks, &mut next);
                    }

                    // No groups at all (empty input): emit a single correctly-typed empty result so
                    // downstream consumers still see the aggregation's output schema.
                    if results.is_empty() {
                        let empty = MicroPartition::empty(Some(params.input_schema.clone()))
                            .agg(&params.original_aggregations, &params.group_by)?;
                        results.push(empty);
                    }

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
        if self.spill_config.is_some() {
            display.push("Spill: enabled (grace aggregation)".to_string());
        }
        display
    }

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(GroupedAggregateState::new(
            self.num_partitions(),
            self.partial_agg_threshold,
            self.high_cardinality_threshold_ratio,
            self.spill_config.as_ref().map(|sc| sc.spill_dirs.clone()),
            self.budget_per_bucket,
        ))
    }
}

/// Finalize one hash bucket: combine its in-memory and spilled data into the final per-group rows.
/// Returns `None` if the bucket is empty.
fn finalize_bucket(
    p: usize,
    parts: Vec<Option<SinglePartitionAggregateState>>,
    stores: Arc<Vec<Option<SpillStore>>>,
    params: Arc<GroupedAggregateParams>,
    recursion_budget: usize,
) -> DaftResult<Option<MicroPartition>> {
    let decomposable = !params.partial_agg_exprs.is_empty();

    let mut in_partials: Vec<MicroPartition> = vec![];
    let mut in_unagg: Vec<MicroPartition> = vec![];
    for st in parts.into_iter().flatten() {
        in_partials.extend(st.partially_aggregated);
        in_unagg.extend(st.unaggregated);
    }

    // Read this bucket's spilled data from every input state.
    let mut spilled: Vec<MicroPartition> = vec![];
    for store in stores.iter().flatten() {
        if store.is_spilled(p) {
            let batches = store.read_bucket(p)?;
            if !batches.is_empty() {
                let schema = batches[0].schema.clone();
                spilled.push(MicroPartition::new_loaded(schema, Arc::new(batches), None));
            }
        }
    }

    if decomposable {
        // Spilled data is already partially aggregated; convert any leftover raw rows to partial.
        if !in_unagg.is_empty() {
            in_partials.push(
                MicroPartition::concat(in_unagg)?
                    .agg(&params.partial_agg_exprs, &params.group_by)?,
            );
        }
        in_partials.extend(spilled);

        let combined = combine_decomposable_partials(
            in_partials,
            &params.final_agg_exprs,
            &params.final_group_by,
            recursion_budget,
            0,
        )?;
        match combined {
            None => Ok(None),
            Some(combined) => Ok(Some(combined.eval_expression_list(&params.final_projections)?)),
        }
    } else {
        // Non-decomposable: aggregate the raw rows for this bucket in one pass. Memory is relieved
        // *between* buckets via spilling, but a single group must still fit in memory.
        let mut raw = in_unagg;
        raw.extend(spilled);
        if raw.is_empty() {
            return Ok(None);
        }
        Ok(Some(MicroPartition::concat(raw)?.agg(
            &params.original_aggregations,
            &params.group_by,
        )?))
    }
}

/// Combine partially-aggregated chunks (all sharing the partial schema) into a single
/// partially-aggregated result (one row per group). If the combined input would exceed `budget`,
/// recursively sub-partitions by the group key (with a per-level hash seed so recursion makes
/// progress) and combines each sub-bucket independently, bounding peak memory.
fn combine_decomposable_partials(
    chunks: Vec<MicroPartition>,
    final_agg_exprs: &[BoundAggExpr],
    final_group_by: &[BoundExpr],
    budget: usize,
    seed: u64,
) -> DaftResult<Option<MicroPartition>> {
    if chunks.is_empty() {
        return Ok(None);
    }
    let total: usize = chunks.iter().map(MicroPartition::size_bytes).sum();
    if seed >= MAX_AGG_RECURSION || total <= budget.max(1) {
        let combined = MicroPartition::concat(chunks)?.agg(final_agg_exprs, final_group_by)?;
        return Ok(Some(combined));
    }

    let sub_n = ((total / budget.max(1)) + 1).clamp(2, 64);
    let mut subs: Vec<Vec<MicroPartition>> = (0..sub_n).map(|_| vec![]).collect();
    for chunk in chunks {
        for (i, sub) in chunk
            .partition_by_hash_seeded(final_group_by, sub_n, seed)?
            .into_iter()
            .enumerate()
        {
            if sub.len() > 0 {
                subs[i].push(sub);
            }
        }
    }

    let mut results = vec![];
    for sub in subs {
        if let Some(combined) =
            combine_decomposable_partials(sub, final_agg_exprs, final_group_by, budget, seed + 1)?
        {
            results.push(combined);
        }
    }
    if results.is_empty() {
        Ok(None)
    } else {
        // Sub-buckets hold disjoint groups, so concatenation is the combined partial result.
        Ok(Some(MicroPartition::concat(results)?))
    }
}
