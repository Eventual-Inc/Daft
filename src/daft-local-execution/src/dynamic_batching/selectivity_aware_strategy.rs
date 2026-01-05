use std::{sync::Arc, time::Duration};

use crate::{
    dynamic_batching::{BatchingState, BatchingStrategy},
    intermediate_ops::filter::FilterStats,
    pipeline::MorselSizeRequirement,
    runtime_stats::RuntimeStats,
};

const MIN_SELECTIVITY: f64 = 0.001; // Prevent extreme amplification
const MAX_AMPLIFICATION: f64 = 1000.0; // Cap amplification factor
const SMOOTHING_FACTOR: f64 = 0.3; // EMA smoothing for selectivity

/// A batching strategy that dynamically adjusts upstream batch size requirements based on
/// observed filter selectivity to prevent downstream operator starvation.
///
/// # Problem
/// When a filter operator has very low selectivity (filters out most rows), small input batches
/// produce even smaller output batches. If a downstream operator (e.g., a UDF) has a strict
/// batch size requirement, it must wait for the filter to process many small batches before
/// receiving enough rows to execute. This causes pipeline stalls and poor throughput.
///
/// # Solution
/// This strategy monitors the filter's selectivity (output_rows / input_rows) and amplifies
/// the upstream batch size requirement accordingly. If selectivity is 1%, the filter requests
/// ~100x more rows from upstream to produce enough output to satisfy downstream requirements.
///
/// # Example
/// - Downstream UDF requires `Strict(100)` rows
/// - Filter has 5% selectivity
/// - Strategy amplifies upstream requirement to `Strict(2000)` rows
/// - Filter processes 2000 rows → outputs ~100 rows → UDF executes immediately
///
/// # Smoothing
/// Uses exponential moving average (EMA) to smooth selectivity measurements across batches,
/// preventing wild swings in batch size from transient selectivity changes.
///
/// # Safety Bounds
/// - `MIN_SELECTIVITY`: Prevents division by zero and extreme amplification
/// - `MAX_AMPLIFICATION`: Caps maximum batch size growth to prevent memory issues
#[derive(Debug, Clone)]
pub struct SelectivityAwareBatchingStrategy {
    downstream_requirement: MorselSizeRequirement,
}

impl SelectivityAwareBatchingStrategy {
    pub fn new(downstream_requirement: MorselSizeRequirement) -> Self {
        Self {
            downstream_requirement,
        }
    }

    fn amplify_requirement(
        &self,
        requirement: MorselSizeRequirement,
        factor: f64,
    ) -> MorselSizeRequirement {
        match requirement {
            MorselSizeRequirement::Strict(size) => {
                let new_size = ((size as f64) * factor).ceil() as usize;
                MorselSizeRequirement::Strict(new_size.max(size))
            }
            MorselSizeRequirement::Flexible(lower, upper) => {
                let new_lower = ((lower as f64) * factor).ceil() as usize;
                let new_upper = ((upper as f64) * factor).ceil() as usize;
                MorselSizeRequirement::Flexible(new_lower.max(lower), new_upper.max(upper))
            }
        }
    }
}

impl BatchingStrategy for SelectivityAwareBatchingStrategy {
    type State = SelectivityState;

    fn make_state(&self) -> Self::State {
        SelectivityState {
            downstream_requirement: self.downstream_requirement,
            input_rows: 0,
            output_rows: 0,
            smoothed_selectivity: None,
        }
    }

    fn calculate_new_requirements(&self, state: &mut Self::State) -> MorselSizeRequirement {
        if let Some(selectivity) = state.smoothed_selectivity {
            let clamped_selectivity = selectivity.max(MIN_SELECTIVITY);
            let amplification = (1.0 / clamped_selectivity).min(MAX_AMPLIFICATION);
            self.amplify_requirement(state.downstream_requirement, amplification)
        } else {
            // No data yet, use downstream requirement as-is
            state.downstream_requirement
        }
    }

    fn initial_requirements(&self) -> MorselSizeRequirement {
        self.downstream_requirement
    }
}

#[derive(Debug)]
pub struct SelectivityState {
    downstream_requirement: MorselSizeRequirement,
    input_rows: u64,
    output_rows: u64,
    smoothed_selectivity: Option<f64>,
}

impl BatchingState for SelectivityState {
    fn record_execution_stat(
        &mut self,
        stats: Arc<dyn RuntimeStats>,
        _batch_size: usize,
        _duration: Duration,
    ) {
        // Downcast to FilterStats to get direct access to counters
        if let Some(filter_stats) = stats.as_any_arc().downcast_ref::<FilterStats>() {
            self.input_rows = filter_stats
                .rows_in
                .load(std::sync::atomic::Ordering::Relaxed);
            self.output_rows = filter_stats
                .rows_out
                .load(std::sync::atomic::Ordering::Relaxed);

            if self.input_rows > 0 {
                let current_selectivity = (self.output_rows as f64) / (self.input_rows as f64);

                self.smoothed_selectivity = Some(match self.smoothed_selectivity {
                    Some(prev) => {
                        // Exponential moving average
                        SMOOTHING_FACTOR
                            .mul_add(current_selectivity, (1.0 - SMOOTHING_FACTOR) * prev)
                    }
                    None => current_selectivity,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn new_filter_stats(rows_in: u64, rows_out: u64) -> FilterStats {
        let stats = FilterStats::new(0);
        stats.add_rows_in(rows_in);
        stats.add_rows_out(rows_out);
        stats
    }

    #[test]
    fn test_initial_requirements_match_downstream() {
        let strategy = SelectivityAwareBatchingStrategy::new(MorselSizeRequirement::Strict(10));
        assert_eq!(
            strategy.initial_requirements(),
            MorselSizeRequirement::Strict(10)
        );

        let strategy =
            SelectivityAwareBatchingStrategy::new(MorselSizeRequirement::Flexible(5, 20));
        assert_eq!(
            strategy.initial_requirements(),
            MorselSizeRequirement::Flexible(5, 20)
        );
    }

    #[test]
    fn test_no_amplification_without_stats() {
        let strategy = SelectivityAwareBatchingStrategy::new(MorselSizeRequirement::Strict(10));
        let mut state = strategy.make_state();

        let requirement = strategy.calculate_new_requirements(&mut state);
        assert_eq!(requirement, MorselSizeRequirement::Strict(10));
    }

    #[test]
    fn test_low_selectivity_amplifies_requirement() {
        let strategy = SelectivityAwareBatchingStrategy::new(MorselSizeRequirement::Strict(10));
        let mut state = strategy.make_state();

        // Simulate 1% selectivity (1000 in, 10 out)
        let stats = Arc::new(new_filter_stats(1000, 10));
        state.record_execution_stat(stats, 1000, Duration::from_millis(100));

        let requirement = strategy.calculate_new_requirements(&mut state);
        match requirement {
            MorselSizeRequirement::Strict(size) => {
                // Should amplify by ~100x for 1% selectivity
                assert!(size > 100, "Expected amplified size > 100, got {}", size);
            }
            _ => panic!("Expected Strict requirement"),
        }
    }

    #[test]
    fn test_high_selectivity_minimal_amplification() {
        let strategy = SelectivityAwareBatchingStrategy::new(MorselSizeRequirement::Strict(10));
        let mut state = strategy.make_state();

        // Simulate 90% selectivity (100 in, 90 out)
        let stats = Arc::new(new_filter_stats(100, 90));
        state.record_execution_stat(stats, 100, Duration::from_millis(100));

        let requirement = strategy.calculate_new_requirements(&mut state);
        match requirement {
            MorselSizeRequirement::Strict(size) => {
                // Should only amplify by ~1.11x for 90% selectivity
                assert!(
                    size >= 10 && size <= 15,
                    "Expected size 10-15, got {}",
                    size
                );
            }
            _ => panic!("Expected Strict requirement"),
        }
    }

    #[test]
    fn test_flexible_requirement_amplification() {
        let strategy =
            SelectivityAwareBatchingStrategy::new(MorselSizeRequirement::Flexible(10, 100));
        let mut state = strategy.make_state();

        // Simulate 10% selectivity
        let stats = Arc::new(new_filter_stats(1000, 100));
        state.record_execution_stat(stats, 1000, Duration::from_millis(100));

        let requirement = strategy.calculate_new_requirements(&mut state);
        match requirement {
            MorselSizeRequirement::Flexible(lower, upper) => {
                // Should amplify by ~10x for 10% selectivity
                assert!(lower >= 100, "Expected lower >= 100, got {}", lower);
                assert!(upper >= 1000, "Expected upper >= 1000, got {}", upper);
            }
            _ => panic!("Expected Flexible requirement"),
        }
    }

    #[test]
    fn test_max_amplification_cap() {
        let strategy = SelectivityAwareBatchingStrategy::new(MorselSizeRequirement::Strict(10));
        let mut state = strategy.make_state();

        // Simulate 0.001% selectivity (should hit max cap)
        let stats = Arc::new(new_filter_stats(100000, 1));
        state.record_execution_stat(stats, 100000, Duration::from_millis(100));

        let requirement = strategy.calculate_new_requirements(&mut state);
        match requirement {
            MorselSizeRequirement::Strict(size) => {
                // Should be capped at MAX_AMPLIFICATION * 10
                assert!(
                    size <= (MAX_AMPLIFICATION as usize) * 10,
                    "Expected cap at {}, got {}",
                    MAX_AMPLIFICATION * 10.0,
                    size
                );
            }
            _ => panic!("Expected Strict requirement"),
        }
    }

    #[test]
    fn test_smoothing_across_multiple_recordings() {
        let strategy = SelectivityAwareBatchingStrategy::new(MorselSizeRequirement::Strict(10));
        let mut state = strategy.make_state();

        // First recording: 50% selectivity
        let stats1 = Arc::new(new_filter_stats(100, 50));

        state.record_execution_stat(stats1, 100, Duration::from_millis(100));
        let first_selectivity = state.smoothed_selectivity.unwrap();

        // Second recording: 10% selectivity
        let stats2 = Arc::new(new_filter_stats(200, 20));
        state.record_execution_stat(stats2, 100, Duration::from_millis(100));
        let second_selectivity = state.smoothed_selectivity.unwrap();

        // Should be smoothed between the two
        assert!(
            second_selectivity < first_selectivity,
            "Selectivity should decrease"
        );
        assert!(
            second_selectivity > 0.1,
            "Should be smoothed, not instantaneous"
        );
    }

    #[test]
    fn test_zero_output_handled_gracefully() {
        let strategy = SelectivityAwareBatchingStrategy::new(MorselSizeRequirement::Strict(10));
        let mut state = strategy.make_state();

        // Simulate 0% selectivity (all filtered)
        let stats = Arc::new(new_filter_stats(1000, 0));
        state.record_execution_stat(stats, 1000, Duration::from_millis(100));

        let requirement = strategy.calculate_new_requirements(&mut state);
        match requirement {
            MorselSizeRequirement::Strict(size) => {
                // Should hit MIN_SELECTIVITY floor and max amplification cap
                assert!(size <= (MAX_AMPLIFICATION as usize) * 10);
            }
            _ => panic!("Expected Strict requirement"),
        }
    }

    #[test]
    fn test_cumulative_stats_update() {
        let strategy = SelectivityAwareBatchingStrategy::new(MorselSizeRequirement::Strict(10));
        let mut state = strategy.make_state();

        // Stats should be cumulative, not incremental
        let stats1 = Arc::new(new_filter_stats(100, 50));
        state.record_execution_stat(stats1, 100, Duration::from_millis(100));

        let stats2 = Arc::new(new_filter_stats(200, 100));
        state.record_execution_stat(stats2, 100, Duration::from_millis(100));

        // State should reflect latest cumulative values
        assert_eq!(state.input_rows, 200);
        assert_eq!(state.output_rows, 100);
        assert_eq!(state.smoothed_selectivity, Some(0.5));
    }
}
