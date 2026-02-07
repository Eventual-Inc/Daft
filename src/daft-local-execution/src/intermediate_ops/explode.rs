use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use common_error::DaftResult;
use common_metrics::{
    CPU_US_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, StatSnapshot, meters::Counter, ops::NodeType,
    snapshot::ExplodeSnapshot,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_functions_list::explode;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use opentelemetry::{KeyValue, global};
use tracing::{Span, instrument};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{
    ExecutionTaskSpawner,
    dynamic_batching::{BatchingState, BatchingStrategy},
    pipeline::{MorselSizeRequirement, NodeName},
    runtime_stats::RuntimeStats,
};

pub struct ExplodeStats {
    cpu_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    node_kv: Vec<KeyValue>,
}

impl ExplodeStats {
    pub fn new(id: usize) -> Self {
        let meter = global::meter("daft.local.node_stats");
        let node_kv = vec![KeyValue::new("node_id", id.to_string())];

        Self {
            cpu_us: Counter::new(&meter, CPU_US_KEY, None),
            rows_in: Counter::new(&meter, ROWS_IN_KEY, None),
            rows_out: Counter::new(&meter, ROWS_OUT_KEY, None),
            node_kv,
        }
    }
}

impl RuntimeStats for ExplodeStats {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: std::sync::atomic::Ordering) -> StatSnapshot {
        let cpu_us = self.cpu_us.load(ordering);
        let rows_in = self.rows_in.load(ordering);
        let rows_out = self.rows_out.load(ordering);

        let amplification = if rows_in == 0 {
            1.
        } else {
            rows_out as f64 / rows_in as f64
        };

        StatSnapshot::Explode(ExplodeSnapshot {
            cpu_us,
            rows_in,
            rows_out,
            amplification,
        })
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.add(cpu_us, self.node_kv.as_slice());
    }
}

pub struct ExplodeOperator {
    to_explode: Arc<Vec<BoundExpr>>,
    index_column: Option<String>,
}

impl ExplodeOperator {
    pub fn new(to_explode: Vec<BoundExpr>, index_column: Option<String>) -> Self {
        Self {
            to_explode: Arc::new(
                to_explode
                    .into_iter()
                    .map(|expr| BoundExpr::new_unchecked(explode(expr.inner().clone())))
                    .collect(),
            ),
            index_column,
        }
    }
}

impl IntermediateOperator for ExplodeOperator {
    type State = ();
    type BatchingStrategy = crate::dynamic_batching::DynBatchingStrategy;
    #[instrument(skip_all, name = "ExplodeOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let to_explode = self.to_explode.clone();
        let index_column = self.index_column.clone();
        task_spawner
            .spawn(
                async move {
                    let out = input.explode(&to_explode, index_column.as_deref())?;
                    Ok((
                        state,
                        IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                    ))
                },
                Span::current(),
            )
            .into()
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!(
            "Explode: {}",
            self.to_explode.iter().map(|e| e.to_string()).join(", ")
        )];
        if let Some(ref idx_col) = self.index_column {
            res.push(format!("Index column = {}", idx_col));
        }
        res
    }

    fn name(&self) -> NodeName {
        "Explode".into()
    }

    fn make_state(&self) -> Self::State {}

    fn op_type(&self) -> NodeType {
        NodeType::Explode
    }

    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(ExplodeStats::new(id))
    }

    fn batching_strategy(
        &self,
        morsel_size_requirement: MorselSizeRequirement,
    ) -> DaftResult<Self::BatchingStrategy> {
        let cfg = daft_context::get_context().execution_config();
        Ok(if cfg.enable_dynamic_batching {
            ExpansionAwareBatchingStrategy::new(morsel_size_requirement).into()
        } else {
            crate::dynamic_batching::StaticBatchingStrategy::new(morsel_size_requirement).into()
        })
    }
}

const MIN_EXPANSION: f64 = 1.0; // Prevent reduction when no expansion
const MAX_REDUCTION: f64 = 0.001; // Cap reduction factor (1000x reduction max)
const SMOOTHING_FACTOR: f64 = 0.3; // EMA smoothing for expansion ratio

/// A batching strategy that dynamically adjusts upstream batch size requirements based on
/// observed explode expansion ratio to prevent excessive downstream batch sizes.
///
/// # Problem
/// When an explode operator has high expansion (one input row produces many output rows),
/// normal-sized input batches produce very large output batches. If a downstream operator
/// has a strict batch size requirement, it receives far more rows than needed, causing
/// memory pressure and inefficient execution.
///
/// # Solution
/// This strategy monitors the explode's expansion ratio (output_rows / input_rows) and
/// reduces the upstream batch size requirement accordingly. If expansion is 100x, the
/// explode requests ~100x fewer rows from upstream to produce the right output size.
///
/// # Example
/// - Downstream operator requires `Strict(100)` rows
/// - Explode has 50x expansion
/// - Strategy reduces upstream requirement to `Strict(2)` rows
/// - Explode processes 2 rows → outputs ~100 rows → downstream executes efficiently
///
/// # Smoothing
/// Uses exponential moving average (EMA) to smooth expansion measurements across batches,
/// preventing wild swings in batch size from transient expansion changes.
///
/// # Safety Bounds
/// - `MIN_EXPANSION`: Prevents amplification when expansion is less than 1x
/// - `MAX_REDUCTION`: Caps minimum batch size to prevent fetching too few rows
#[derive(Debug, Clone)]
struct ExpansionAwareBatchingStrategy {
    downstream_requirement: MorselSizeRequirement,
}

impl ExpansionAwareBatchingStrategy {
    pub fn new(downstream_requirement: MorselSizeRequirement) -> Self {
        Self {
            downstream_requirement,
        }
    }

    fn reduce_requirement(
        &self,
        requirement: MorselSizeRequirement,
        factor: f64,
    ) -> MorselSizeRequirement {
        match requirement {
            MorselSizeRequirement::Strict(size) => {
                let new_size = ((size.get() as f64) * factor).ceil() as usize;
                MorselSizeRequirement::Strict(NonZeroUsize::new(new_size.max(1)).unwrap())
            }
            MorselSizeRequirement::Flexible(lower, upper) => {
                let new_lower = ((lower as f64) * factor).ceil() as usize;
                let new_upper = ((upper.get() as f64) * factor).ceil() as usize;
                MorselSizeRequirement::Flexible(
                    new_lower.max(1),
                    NonZeroUsize::new(new_upper.max(1)).unwrap(),
                )
            }
        }
    }
}

impl BatchingStrategy for ExpansionAwareBatchingStrategy {
    type State = ExpansionState;

    fn make_state(&self) -> Self::State {
        ExpansionState {
            downstream_requirement: self.downstream_requirement,
            input_rows: 0,
            output_rows: 0,
            smoothed_expansion: None,
        }
    }

    fn calculate_new_requirements(&self, state: &mut Self::State) -> MorselSizeRequirement {
        if let Some(expansion) = state.smoothed_expansion {
            let clamped_expansion = expansion.max(MIN_EXPANSION);
            let reduction = (1.0 / clamped_expansion).max(MAX_REDUCTION);
            self.reduce_requirement(state.downstream_requirement, reduction)
        } else {
            state.downstream_requirement
        }
    }

    fn initial_requirements(&self) -> MorselSizeRequirement {
        self.downstream_requirement
    }
}

#[derive(Debug)]
struct ExpansionState {
    downstream_requirement: MorselSizeRequirement,
    input_rows: u64,
    output_rows: u64,
    smoothed_expansion: Option<f64>,
}

impl BatchingState for ExpansionState {
    fn record_execution_stat(
        &mut self,
        stats: &dyn RuntimeStats,
        _batch_size: usize,
        _duration: Duration,
    ) {
        if let Some(explode_stats) = stats.as_any().downcast_ref::<ExplodeStats>() {
            self.input_rows = explode_stats
                .rows_in
                .load(std::sync::atomic::Ordering::Relaxed);
            self.output_rows = explode_stats
                .rows_out
                .load(std::sync::atomic::Ordering::Relaxed);

            if self.input_rows > 0 {
                let current_expansion = (self.output_rows as f64) / (self.input_rows as f64);

                self.smoothed_expansion = Some(match self.smoothed_expansion {
                    Some(prev) => {
                        SMOOTHING_FACTOR.mul_add(current_expansion, (1.0 - SMOOTHING_FACTOR) * prev)
                    }
                    None => current_expansion,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn new_explode_stats(rows_in: u64, rows_out: u64) -> ExplodeStats {
        let stats = ExplodeStats::new(0);
        stats.add_rows_in(rows_in);
        stats.add_rows_out(rows_out);
        stats
    }

    #[test]
    fn test_initial_requirements_match_downstream() {
        let strategy = ExpansionAwareBatchingStrategy::new(MorselSizeRequirement::Strict(
            NonZeroUsize::new(10).unwrap(),
        ));
        assert_eq!(
            strategy.initial_requirements(),
            MorselSizeRequirement::Strict(NonZeroUsize::new(10).unwrap())
        );

        let strategy = ExpansionAwareBatchingStrategy::new(MorselSizeRequirement::Flexible(
            5,
            NonZeroUsize::new(20).unwrap(),
        ));
        assert_eq!(
            strategy.initial_requirements(),
            MorselSizeRequirement::Flexible(5, NonZeroUsize::new(20).unwrap())
        );
    }

    #[test]
    fn test_no_reduction_without_stats() {
        let strategy = ExpansionAwareBatchingStrategy::new(MorselSizeRequirement::Strict(
            NonZeroUsize::new(10).unwrap(),
        ));
        let mut state = strategy.make_state();

        let requirement = strategy.calculate_new_requirements(&mut state);
        assert_eq!(
            requirement,
            MorselSizeRequirement::Strict(NonZeroUsize::new(10).unwrap())
        );
    }

    #[test]
    fn test_high_expansion_reduces_requirement() {
        let strategy = ExpansionAwareBatchingStrategy::new(MorselSizeRequirement::Strict(
            NonZeroUsize::new(1000).unwrap(),
        ));
        let mut state = strategy.make_state();

        // Simulate 100x expansion (10 in, 1000 out)
        let stats: Arc<dyn RuntimeStats> = Arc::new(new_explode_stats(10, 1000));
        state.record_execution_stat(stats.as_ref(), 10, Duration::from_millis(100));

        let requirement = strategy.calculate_new_requirements(&mut state);
        match requirement {
            MorselSizeRequirement::Strict(size) => {
                // Should reduce by ~100x for 100x expansion
                assert!(size.get() < 20, "Expected reduced size < 20, got {}", size);
            }
            _ => panic!("Expected Strict requirement"),
        }
    }

    #[test]
    fn test_low_expansion_minimal_reduction() {
        let strategy = ExpansionAwareBatchingStrategy::new(MorselSizeRequirement::Strict(
            NonZeroUsize::new(100).unwrap(),
        ));
        let mut state = strategy.make_state();

        // Simulate 1.1x expansion (100 in, 110 out)
        let stats: Arc<dyn RuntimeStats> = Arc::new(new_explode_stats(100, 110));
        state.record_execution_stat(stats.as_ref(), 100, Duration::from_millis(100));

        let requirement = strategy.calculate_new_requirements(&mut state);
        match requirement {
            MorselSizeRequirement::Strict(size) => {
                // Should only reduce by ~1.1x for 1.1x expansion
                assert!(
                    size.get() >= 85 && size.get() <= 100,
                    "Expected size 85-100, got {}",
                    size
                );
            }
            _ => panic!("Expected Strict requirement"),
        }
    }

    #[test]
    fn test_flexible_requirement_reduction() {
        let strategy = ExpansionAwareBatchingStrategy::new(MorselSizeRequirement::Flexible(
            100,
            NonZeroUsize::new(1000).unwrap(),
        ));
        let mut state = strategy.make_state();

        // Simulate 10x expansion
        let stats = Arc::new(new_explode_stats(100, 1000));
        state.record_execution_stat(stats.as_ref(), 100, Duration::from_millis(100));

        let requirement = strategy.calculate_new_requirements(&mut state);
        match requirement {
            MorselSizeRequirement::Flexible(lower, upper) => {
                // Should reduce by ~10x for 10x expansion
                assert!(lower <= 15, "Expected lower <= 15, got {}", lower);
                assert!(upper.get() <= 150, "Expected upper <= 150, got {}", upper);
            }
            _ => panic!("Expected Flexible requirement"),
        }
    }

    #[test]
    fn test_max_reduction_cap() {
        let strategy = ExpansionAwareBatchingStrategy::new(MorselSizeRequirement::Strict(
            NonZeroUsize::new(10000).unwrap(),
        ));
        let mut state = strategy.make_state();

        // Simulate 100000x expansion (should hit max cap)
        let stats: Arc<dyn RuntimeStats> = Arc::new(new_explode_stats(1, 100000));
        state.record_execution_stat(stats.as_ref(), 1, Duration::from_millis(100));

        let requirement = strategy.calculate_new_requirements(&mut state);
        match requirement {
            MorselSizeRequirement::Strict(size) => {
                // Should be capped at MAX_REDUCTION * 10000
                assert!(
                    size.get() >= (MAX_REDUCTION * 10000.0) as usize,
                    "Expected floor at {}, got {}",
                    MAX_REDUCTION * 10000.0,
                    size
                );
            }
            _ => panic!("Expected Strict requirement"),
        }
    }

    #[test]
    fn test_smoothing_across_multiple_recordings() {
        let strategy = ExpansionAwareBatchingStrategy::new(MorselSizeRequirement::Strict(
            NonZeroUsize::new(1000).unwrap(),
        ));
        let mut state = strategy.make_state();

        // First recording: 2x expansion
        let stats1: Arc<dyn RuntimeStats> = Arc::new(new_explode_stats(100, 200));
        state.record_execution_stat(stats1.as_ref(), 100, Duration::from_millis(100));
        let first_expansion = state.smoothed_expansion.unwrap();

        // Second recording: 10x expansion
        let stats2: Arc<dyn RuntimeStats> = Arc::new(new_explode_stats(200, 2000));
        state.record_execution_stat(stats2.as_ref(), 100, Duration::from_millis(100));
        let second_expansion = state.smoothed_expansion.unwrap();

        // Should be smoothed between the two
        assert!(
            second_expansion > first_expansion,
            "Expansion should increase"
        );
        assert!(
            second_expansion < 10.0,
            "Should be smoothed, not instantaneous"
        );
    }

    #[test]
    fn test_no_expansion_handled_gracefully() {
        let strategy = ExpansionAwareBatchingStrategy::new(MorselSizeRequirement::Strict(
            NonZeroUsize::new(100).unwrap(),
        ));
        let mut state = strategy.make_state();

        // Simulate 1x expansion (no change)
        let stats: Arc<dyn RuntimeStats> = Arc::new(new_explode_stats(100, 100));
        state.record_execution_stat(stats.as_ref(), 100, Duration::from_millis(100));

        let requirement = strategy.calculate_new_requirements(&mut state);
        match requirement {
            MorselSizeRequirement::Strict(size) => {
                // Should not reduce with 1x expansion
                assert_eq!(size.get(), 100);
            }
            _ => panic!("Expected Strict requirement"),
        }
    }

    #[test]
    fn test_cumulative_stats_update() {
        let strategy = ExpansionAwareBatchingStrategy::new(MorselSizeRequirement::Strict(
            NonZeroUsize::new(1000).unwrap(),
        ));
        let mut state = strategy.make_state();

        // Stats should be cumulative, not incremental
        let stats1 = Arc::new(new_explode_stats(100, 200));
        state.record_execution_stat(stats1.as_ref(), 100, Duration::from_millis(100));

        let stats2 = Arc::new(new_explode_stats(200, 400));
        state.record_execution_stat(stats2.as_ref(), 100, Duration::from_millis(100));

        // State should reflect latest cumulative values
        assert_eq!(state.input_rows, 200);
        assert_eq!(state.output_rows, 400);
        assert_eq!(state.smoothed_expansion, Some(2.0));
    }
}
