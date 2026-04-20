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
            state.partially_aggregated.push(p);
            Self::try_eager_combine(state, params)?;
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
                Self::try_eager_combine(state, params)?;
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

    /// Map-side combine to keep memory bounded. Uses `agg_combine_only` (not `agg`) so
    /// `AggFnReduce` columns stay as Struct and remain valid input for the final `agg()`.
    fn try_eager_combine(
        state: &mut SinglePartitionAggregateState,
        params: &GroupedAggregateParams,
    ) -> DaftResult<()> {
        let needs_combine = params
            .final_agg_exprs
            .iter()
            .any(|e| matches!(e.as_ref(), daft_dsl::AggExpr::AggFnReduce { .. }));
        if !needs_combine || state.partially_aggregated.len() < PARTIAL_AGG_COMBINE_THRESHOLD {
            return Ok(());
        }
        let partitions = std::mem::take(&mut state.partially_aggregated);
        let concated = MicroPartition::concat(partitions)?;
        let combined =
            concated.agg_combine_only(&params.final_agg_exprs, &params.final_group_by)?;
        state.partially_aggregated = vec![combined];
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
        input: MicroPartition,
        params: &GroupedAggregateParams,
        global_strategy_lock: &Arc<Mutex<Option<AggStrategy>>>,
    ) -> DaftResult<()> {
        let Self::Accumulating {
            inner_states,
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

    fn finalize(&mut self) -> Vec<Option<SinglePartitionAggregateState>> {
        let res = if let Self::Accumulating { inner_states, .. } = self {
            std::mem::take(inner_states)
        } else {
            panic!("GroupedAggregateSink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

/// How many partial-aggregate partitions may accumulate per hash-bucket before
/// an eager map-side combine is triggered. Only active when `AggFn` expressions are present.
const PARTIAL_AGG_COMBINE_THRESHOLD: usize = 4;

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
                    let mut state_iters = states
                        .into_iter()
                        .map(|mut state| state.finalize().into_iter())
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
                            if params.partial_agg_exprs.is_empty() && !unaggregated.is_empty() {
                                let concated = MicroPartition::concat(unaggregated)?;
                                let agged = concated
                                    .agg(&params.original_aggregations, &params.group_by)?;
                                Ok(agged)
                            }
                            // If we have no unaggregated partitions, finalize the partially aggregated partitions
                            else if unaggregated.is_empty() {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_dsl::{
        AggExpr,
        expr::bound_expr::BoundAggExpr,
        functions::{AggFn, AggFnHandle},
        unresolved_col,
    };
    use daft_micropartition::MicroPartition;
    use daft_recordbatch::RecordBatch;

    use super::{
        AggStrategy, GroupedAggregateParams, PARTIAL_AGG_COMBINE_THRESHOLD,
        SinglePartitionAggregateState,
    };

    #[derive(serde::Serialize, serde::Deserialize)]
    struct TestSumAggSink;

    #[typetag::serde(name = "TestSumAggSink")]
    impl AggFn for TestSumAggSink {
        fn name(&self) -> &'static str {
            "test_sum_sink"
        }

        fn get_return_field(&self, _inputs: &[Field], _schema: &Schema) -> DaftResult<Field> {
            Ok(Field::new("x", DataType::Int64))
        }

        fn state_fields(&self, _inputs: &[Field]) -> DaftResult<Vec<Field>> {
            Ok(vec![Field::new("sum", DataType::Int64)])
        }

        fn call_agg_block(
            &self,
            inputs: Vec<Series>,
            groups: Option<&daft_core::array::ops::GroupIndices>,
        ) -> DaftResult<Vec<Series>> {
            let partial = inputs[0].sum(groups)?;
            let sums: Vec<i64> = partial.i64()?.into_iter().map(|v| v.unwrap_or(0)).collect();
            Ok(vec![Int64Array::from_vec("sum", sums).into_series()])
        }

        fn call_agg_combine(
            &self,
            states: Vec<Series>,
            groups: Option<&daft_core::array::ops::GroupIndices>,
        ) -> DaftResult<Vec<Series>> {
            let merged = states[0].sum(groups)?;
            let sums: Vec<i64> = merged.i64()?.into_iter().map(|v| v.unwrap_or(0)).collect();
            Ok(vec![Int64Array::from_vec("sum", sums).into_series()])
        }

        fn call_agg_finalize(&self, states: Vec<Series>, return_field: &Field) -> DaftResult<Series> {
            let values: Vec<i64> = states[0].i64()?.into_iter().map(|v| v.unwrap_or(0)).collect();
            Ok(Int64Array::from_vec(&return_field.name, values).into_series())
        }
    }

    fn make_handle() -> AggFnHandle {
        AggFnHandle::new(Arc::new(TestSumAggSink))
    }

    fn partial_col_name() -> String {
        "test_sum_sink(x)".to_string()
    }

    fn state_dtype() -> DataType {
        DataType::Struct(vec![Field::new("sum", DataType::Int64)])
    }

    fn make_struct_mp(value: i64) -> MicroPartition {
        let col_name = partial_col_name();
        let sum_series = Int64Array::from_vec("sum", vec![value]).into_series();
        let struct_field = Field::new(col_name.as_str(), state_dtype());
        let struct_series = StructArray::new(struct_field, vec![sum_series], None).into_series();
        let rb = RecordBatch::from_nonempty_columns(vec![struct_series]).unwrap();
        MicroPartition::new_loaded(rb.schema.clone(), Arc::new(vec![rb]), None)
    }

    fn make_reduce_params() -> GroupedAggregateParams {
        let col_name = partial_col_name();
        let schema = Arc::new(Schema::new(std::iter::once(Field::new(
            col_name.as_str(),
            state_dtype(),
        ))));
        let return_field = Field::new("x", DataType::Int64);
        let final_agg = BoundAggExpr::try_new(
            AggExpr::AggFnReduce {
                handle: make_handle(),
                partial: unresolved_col(col_name.as_str()),
                return_field,
            },
            &schema,
        )
        .unwrap();
        GroupedAggregateParams {
            original_aggregations: vec![],
            group_by: vec![],
            partial_agg_exprs: vec![],
            final_agg_exprs: vec![final_agg],
            final_group_by: vec![],
            final_projections: vec![],
        }
    }

    fn make_empty_params() -> GroupedAggregateParams {
        GroupedAggregateParams {
            original_aggregations: vec![],
            group_by: vec![],
            partial_agg_exprs: vec![],
            final_agg_exprs: vec![],
            final_group_by: vec![],
            final_projections: vec![],
        }
    }

    #[test]
    fn test_try_eager_combine_below_threshold() -> DaftResult<()> {
        let params = make_reduce_params();
        let mut state = SinglePartitionAggregateState::default();
        for i in 1..(PARTIAL_AGG_COMBINE_THRESHOLD as i64) {
            state.partially_aggregated.push(make_struct_mp(i));
        }
        let before = state.partially_aggregated.len();
        AggStrategy::try_eager_combine(&mut state, &params)?;
        assert_eq!(state.partially_aggregated.len(), before);
        Ok(())
    }

    #[test]
    fn test_try_eager_combine_no_agg_fn_reduce() -> DaftResult<()> {
        let params = make_empty_params();
        let mut state = SinglePartitionAggregateState::default();
        for i in 0..(PARTIAL_AGG_COMBINE_THRESHOLD as i64 + 2) {
            state.partially_aggregated.push(make_struct_mp(i));
        }
        let before = state.partially_aggregated.len();
        AggStrategy::try_eager_combine(&mut state, &params)?;
        assert_eq!(state.partially_aggregated.len(), before);
        Ok(())
    }

    #[test]
    fn test_try_eager_combine_triggers() -> DaftResult<()> {
        let params = make_reduce_params();
        let mut state = SinglePartitionAggregateState::default();
        for i in 1..=(PARTIAL_AGG_COMBINE_THRESHOLD as i64) {
            state.partially_aggregated.push(make_struct_mp(i));
        }
        AggStrategy::try_eager_combine(&mut state, &params)?;
        assert_eq!(state.partially_aggregated.len(), 1);

        // Finalize the combined struct state — should be 1+2+3+4 = 10.
        let col_name = partial_col_name();
        let schema = Arc::new(Schema::new(std::iter::once(Field::new(
            col_name.as_str(),
            state_dtype(),
        ))));
        let return_field = Field::new("x", DataType::Int64);
        let final_agg = BoundAggExpr::try_new(
            AggExpr::AggFnReduce {
                handle: make_handle(),
                partial: unresolved_col(col_name.as_str()),
                return_field,
            },
            &schema,
        )
        .unwrap();
        let finalized = state.partially_aggregated[0].agg(&[final_agg], &[])?;
        let rb = &finalized.record_batches()[0];
        let col = daft_recordbatch::get_column_by_name(rb, "x")?;
        assert_eq!(col.i64()?.get(0), Some(10i64));
        Ok(())
    }

    // Verifies that the Struct output of one combine round is valid input for a
    // subsequent round: the combined state must survive re-entry into call_agg_combine.
    #[test]
    fn test_try_eager_combine_multiple_rounds() -> DaftResult<()> {
        let params = make_reduce_params();
        let mut state = SinglePartitionAggregateState::default();

        // Round 1: push exactly threshold MPs (1+2+3+4=10) → collapses to 1.
        for i in 1..=(PARTIAL_AGG_COMBINE_THRESHOLD as i64) {
            state.partially_aggregated.push(make_struct_mp(i));
        }
        AggStrategy::try_eager_combine(&mut state, &params)?;
        assert_eq!(state.partially_aggregated.len(), 1);

        // Round 2: add 3 more MPs (5+6+7) so total len=4 hits the threshold again.
        for i in 5..=(PARTIAL_AGG_COMBINE_THRESHOLD as i64 + 3) {
            state.partially_aggregated.push(make_struct_mp(i));
        }
        AggStrategy::try_eager_combine(&mut state, &params)?;
        assert_eq!(state.partially_aggregated.len(), 1);

        // Finalize — expected: 1+2+3+4+5+6+7 = 28.
        let col_name = partial_col_name();
        let schema = Arc::new(Schema::new(std::iter::once(Field::new(
            col_name.as_str(),
            state_dtype(),
        ))));
        let return_field = Field::new("x", DataType::Int64);
        let final_agg = BoundAggExpr::try_new(
            AggExpr::AggFnReduce {
                handle: make_handle(),
                partial: unresolved_col(col_name.as_str()),
                return_field,
            },
            &schema,
        )
        .unwrap();
        let finalized = state.partially_aggregated[0].agg(&[final_agg], &[])?;
        let rb = &finalized.record_batches()[0];
        let col = daft_recordbatch::get_column_by_name(rb, "x")?;
        assert_eq!(col.i64()?.get(0), Some(28i64));
        Ok(())
    }
}
