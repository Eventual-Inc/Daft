mod dyn_strategy;
mod latency_constrained_strategy;
mod static_strategy;
use std::{sync::Arc, time::Duration};

pub use dyn_strategy::*;
pub use latency_constrained_strategy::*;
use parking_lot::Mutex;
pub use static_strategy::*;

use crate::{pipeline::MorselSizeRequirement, runtime_stats::RuntimeStats};

/// Trait for algorithms that dynamically adjust batch sizes based on execution performance.
///
/// Dynamic batching is a technique used to optimize throughput and latency by adjusting
/// the number of items processed together in a batch based on runtime performance metrics.
/// Different algorithms use various strategies to find the optimal batch size.
#[cfg(not(debug_assertions))]
pub trait BatchingStrategy: Send + Sync {
    type State: BatchingState + Send + Sync + Unpin;
    fn make_state(&self) -> Self::State;
    /// adjust the batch size based on runtime performance metrics
    fn calculate_new_requirements(&self, state: &mut Self::State) -> MorselSizeRequirement;

    fn initial_requirements(&self) -> MorselSizeRequirement;
}

#[cfg(debug_assertions)]
pub trait BatchingStrategy: Send + Sync + std::fmt::Debug {
    type State: BatchingState + Send + Sync + Unpin;
    fn make_state(&self) -> Self::State;
    fn calculate_new_requirements(&self, state: &mut Self::State) -> MorselSizeRequirement;
    fn initial_requirements(&self) -> MorselSizeRequirement;
}

pub trait BatchingState {
    fn record_execution_stat(
        &mut self,
        stats: Arc<dyn RuntimeStats>,
        batch_size: usize,
        duration: Duration,
    );
}

/// Manages dynamic batch sizing by collecting execution metrics and adjusting
/// batch requirements using pluggable batching strategies.
///
/// Reports are collected asynchronously in the background, and batch size
/// calculations are performed lazily when `calculate_batch_size()` is called.
/// This allows workers to report metrics without blocking while the dispatcher
/// gets updated requirements on-demand.
///
/// # Usage
/// ```rust,ignore
/// let manager = BatchManager::new(strategy);
///
/// manager.record_execution_stats(stats, batch_size, duration);
///
/// let requirements = manager.calculate_batch_size();
/// ```
pub struct BatchManager<S: BatchingStrategy> {
    state: Arc<Mutex<S::State>>,
    pub(crate) strategy: S,
}

impl<S> BatchManager<S>
where
    S: BatchingStrategy + 'static,
    S::State: 'static,
{
    pub fn new(strategy: S) -> Self {
        let state = strategy.make_state();
        let state = Arc::new(Mutex::new(state));

        Self { state, strategy }
    }
    /// Processes pending execution reports and returns updated batch size requirements.
    ///
    /// This method triggers lazy evaluation - it processes all reports collected since
    /// the last call and updates the internal requirements accordingly. If no new reports
    /// are available, returns the current cached requirements.
    pub fn calculate_batch_size(&self) -> MorselSizeRequirement {
        let mut state = self.state.lock();
        self.strategy.calculate_new_requirements(&mut state)
    }

    /// Records execution metrics for a processed batch.
    ///
    /// Sends the execution stats asynchronously to a background task for later processing.
    /// This method is non-blocking and designed to be called frequently by workers.
    pub fn record_execution_stats(
        &self,
        stats: Arc<dyn RuntimeStats>,
        batch_size: usize,
        duration: Duration,
    ) {
        let mut state = self.state.lock();
        state.record_execution_stat(stats, batch_size, duration);
    }

    pub fn initial_requirements(&self) -> MorselSizeRequirement {
        self.strategy.initial_requirements()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;
    use crate::runtime_stats::RuntimeStats;

    // Mock RuntimeStats for testing
    pub(crate) struct MockRuntimeStats;
    impl RuntimeStats for MockRuntimeStats {
        fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
            unimplemented!()
        }

        fn build_snapshot(
            &self,
            _ordering: std::sync::atomic::Ordering,
        ) -> common_metrics::StatSnapshot {
            unimplemented!()
        }

        fn add_rows_in(&self, _rows: u64) {
            unimplemented!()
        }

        fn add_rows_out(&self, _rows: u64) {
            unimplemented!()
        }

        fn add_cpu_us(&self, _cpu_us: u64) {
            unimplemented!()
        }
    }

    // Mock state that tracks measurements
    struct MockBatchingState {
        measurement_count: usize,
    }

    impl BatchingState for MockBatchingState {
        fn record_execution_stat(
            &mut self,
            _stats: Arc<dyn RuntimeStats>,
            _batch_size: usize,
            _duration: Duration,
        ) {
            self.measurement_count += 1;
        }
    }

    // Mock strategy that tracks calls and returns different requirements
    #[derive(Clone, Debug)]
    struct MockBatchingStrategy {
        call_counter: Arc<AtomicUsize>,
        initial_req: MorselSizeRequirement,
    }

    impl MockBatchingStrategy {
        fn new(initial_req: MorselSizeRequirement) -> Self {
            Self {
                call_counter: Arc::new(AtomicUsize::new(0)),
                initial_req,
            }
        }

        fn call_count(&self) -> usize {
            self.call_counter.load(Ordering::SeqCst)
        }
    }

    impl BatchingStrategy for MockBatchingStrategy {
        type State = MockBatchingState;

        fn make_state(&self) -> Self::State {
            MockBatchingState {
                measurement_count: 0,
            }
        }

        fn initial_requirements(&self) -> MorselSizeRequirement {
            self.initial_req
        }

        fn calculate_new_requirements(&self, state: &mut Self::State) -> MorselSizeRequirement {
            self.call_counter.fetch_add(1, Ordering::SeqCst);

            // Return different requirements based on number of measurements recorded
            match state.measurement_count {
                0 => self.initial_req,
                1 => MorselSizeRequirement::Flexible(1, 10),
                2..=5 => MorselSizeRequirement::Flexible(5, 20),
                _ => MorselSizeRequirement::Flexible(10, 50),
            }
        }
    }

    #[test]
    fn test_batch_manager_creation() {
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(1, 32));
        let manager = BatchManager::new(strategy.clone());

        assert_eq!(
            manager.initial_requirements(),
            MorselSizeRequirement::Flexible(1, 32)
        );
        assert_eq!(strategy.call_count(), 0); // No calculations yet
    }

    #[test]
    fn test_batch_manager_calculate_no_measurements() {
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(2, 64));
        let manager = BatchManager::new(strategy.clone());

        let req = manager.calculate_batch_size();
        assert_eq!(req, MorselSizeRequirement::Flexible(2, 64)); // Should return initial
        assert_eq!(strategy.call_count(), 1); // calculate_new_requirements is always called
    }

    #[test]
    fn test_batch_manager_record_and_calculate() {
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(1, 16));
        let manager = BatchManager::new(strategy.clone());

        // Record some execution stats
        manager.record_execution_stats(Arc::new(MockRuntimeStats), 32, Duration::from_millis(100));

        let req = manager.calculate_batch_size();
        assert_eq!(req, MorselSizeRequirement::Flexible(1, 10)); // First state transition
        assert_eq!(strategy.call_count(), 1);
    }

    #[test]
    fn test_batch_manager_multiple_measurements() {
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(1, 8));
        let manager = BatchManager::new(strategy.clone());

        // Record multiple stats
        manager.record_execution_stats(Arc::new(MockRuntimeStats), 10, Duration::from_millis(50));
        manager.record_execution_stats(Arc::new(MockRuntimeStats), 20, Duration::from_millis(75));

        let req = manager.calculate_batch_size();
        assert_eq!(req, MorselSizeRequirement::Flexible(5, 20)); // 2 measurements processed
        assert_eq!(strategy.call_count(), 1);
    }

    #[test]
    fn test_batch_manager_multiple_calculations() {
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(1, 4));
        let manager = BatchManager::new(strategy.clone());

        manager.record_execution_stats(Arc::new(MockRuntimeStats), 5, Duration::from_millis(25));

        let req1 = manager.calculate_batch_size();
        assert_eq!(req1, MorselSizeRequirement::Flexible(1, 10));
        assert_eq!(strategy.call_count(), 1);

        // Second call should trigger calculation again
        let req2 = manager.calculate_batch_size();
        assert_eq!(req2, MorselSizeRequirement::Flexible(1, 10)); // Same result since no new measurements
        assert_eq!(strategy.call_count(), 2); // Called again
    }

    #[test]
    fn test_batch_manager_accumulates_measurements() {
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(2, 8));
        let manager = BatchManager::new(strategy.clone());

        // First measurement
        manager.record_execution_stats(Arc::new(MockRuntimeStats), 10, Duration::from_millis(30));
        let req1 = manager.calculate_batch_size();
        assert_eq!(req1, MorselSizeRequirement::Flexible(1, 10));

        // More measurements
        manager.record_execution_stats(Arc::new(MockRuntimeStats), 15, Duration::from_millis(40));
        manager.record_execution_stats(Arc::new(MockRuntimeStats), 20, Duration::from_millis(60));
        let req2 = manager.calculate_batch_size();
        assert_eq!(req2, MorselSizeRequirement::Flexible(5, 20)); // 3 total measurements

        assert_eq!(strategy.call_count(), 2);
    }

    #[test]
    fn test_batch_manager_with_static_strategy() {
        let static_req = MorselSizeRequirement::Flexible(16, 128);
        let strategy = StaticBatchingStrategy::new(static_req);
        let manager = BatchManager::new(strategy);

        assert_eq!(manager.initial_requirements(), static_req);

        // Even after recording stats, static strategy should return same requirement
        manager.record_execution_stats(Arc::new(MockRuntimeStats), 64, Duration::from_millis(200));

        let req = manager.calculate_batch_size();
        assert_eq!(req, static_req);
    }
}
