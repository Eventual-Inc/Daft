mod dyn_strategy;
mod latency_constrained_strategy;
mod static_strategy;
use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwap;
pub use dyn_strategy::*;
pub use latency_constrained_strategy::*;
pub use static_strategy::*;
use tokio::sync::Mutex;

use crate::{
    RuntimeHandle,
    channel::{Sender, create_channel},
    pipeline::MorselSizeRequirement,
    runtime_stats::RuntimeStats,
};
type BatchReport = Vec<(Arc<dyn RuntimeStats>, usize, Duration)>;

/// Trait for algorithms that dynamically adjust batch sizes based on execution performance.
///
/// Dynamic batching is a technique used to optimize throughput and latency by adjusting
/// the number of items processed together in a batch based on runtime performance metrics.
/// Different algorithms use various strategies to find the optimal batch size.
#[cfg(not(debug_assertions))]
pub trait BatchingStrategy: Send + Sync {
    type State: Send + Sync + Unpin;
    fn make_state(&self) -> Self::State;
    /// adjust the batch size based on runtime performance metrics
    fn calculate_new_requirements(
        &self,
        state: &mut Self::State,
        reports: BatchReport,
    ) -> MorselSizeRequirement;

    fn initial_requirements(&self) -> MorselSizeRequirement;
}

#[cfg(debug_assertions)]
pub trait BatchingStrategy: Send + Sync + std::fmt::Debug {
    type State: Send + Sync + Unpin;
    fn make_state(&self) -> Self::State;
    fn calculate_new_requirements(
        &self,
        state: &mut Self::State,
        reports: BatchReport,
    ) -> MorselSizeRequirement;

    fn initial_requirements(&self) -> MorselSizeRequirement;
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
/// let manager = BatchManager::new(strategy, &runtime_handle);
///
/// manager.record_execution_stats(stats, batch_size, duration).await;
///
/// let requirements = manager.calculate_batch_size().await;
/// ```
pub struct BatchManager<S: BatchingStrategy> {
    requirements: ArcSwap<MorselSizeRequirement>,
    // TODO: all we should need is to look at the runtime stats, but currently runtime stats only provides a cumulative value
    // If we update runtimestats to have a windowed value of recent reports, we can use that alone to calculate the new requirements
    pending_reports: Arc<Mutex<BatchReport>>,
    state: Arc<Mutex<S::State>>,
    pub(crate) strategy: S,
    tx: Sender<(Arc<dyn RuntimeStats>, usize, Duration)>,
}

impl<S> BatchManager<S>
where
    S: BatchingStrategy + 'static,
    S::State: 'static,
{
    pub fn new(strategy: S, runtime_handle: &RuntimeHandle) -> Self {
        let (tx, rx) = create_channel::<(Arc<dyn RuntimeStats>, usize, Duration)>(0);
        let state = strategy.make_state();
        let requirements = strategy.initial_requirements();
        let requirements = ArcSwap::new(Arc::new(requirements));
        let state = Arc::new(Mutex::new(state));
        let pending_reports = Arc::new(Mutex::new(Vec::new()));

        let pending_reports_clone = pending_reports.clone();
        runtime_handle.spawn(async move {
            while let Some(report) = rx.recv().await {
                pending_reports_clone.lock().await.push(report);
            }
        });

        Self {
            requirements,
            pending_reports,
            state,
            strategy,
            tx,
        }
    }
    /// Processes pending execution reports and returns updated batch size requirements.
    ///
    /// This method triggers lazy evaluation - it processes all reports collected since
    /// the last call and updates the internal requirements accordingly. If no new reports
    /// are available, returns the current cached requirements.
    pub async fn calculate_batch_size(&self) -> MorselSizeRequirement {
        // Drain pending reports and process them all
        let mut reports = self.pending_reports.lock().await;

        if !reports.is_empty() {
            let mut state = self.state.lock().await;
            let new_reqs = self
                .strategy
                .calculate_new_requirements(&mut state, std::mem::take(&mut *reports));
            self.requirements.store(Arc::new(new_reqs));
        }

        *self.requirements.load().as_ref()
    }

    /// Records execution metrics for a processed batch.
    ///
    /// Sends the execution stats asynchronously to a background task for later processing.
    /// This method is non-blocking and designed to be called frequently by workers.
    pub async fn record_execution_stats(
        &self,
        stats: Arc<dyn RuntimeStats>,
        batch_size: usize,
        duration: Duration,
    ) {
        self.tx.send((stats, batch_size, duration)).await.unwrap();
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
        type State = usize;

        fn make_state(&self) -> Self::State {
            0
        }

        fn initial_requirements(&self) -> MorselSizeRequirement {
            self.initial_req
        }

        fn calculate_new_requirements(
            &self,
            state: &mut Self::State,
            reports: BatchReport,
        ) -> MorselSizeRequirement {
            self.call_counter.fetch_add(1, Ordering::SeqCst);
            *state += reports.len();

            // Return different requirements based on number of reports processed
            match *state {
                0 => self.initial_req,
                1 => MorselSizeRequirement::Flexible(1, 10),
                2..=5 => MorselSizeRequirement::Flexible(5, 20),
                _ => MorselSizeRequirement::Flexible(10, 50),
            }
        }
    }

    #[tokio::test]
    async fn test_batch_manager_creation() {
        let handle = RuntimeHandle::new();
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(1, 32));
        let manager = BatchManager::new(strategy.clone(), &handle);

        assert_eq!(
            manager.initial_requirements(),
            MorselSizeRequirement::Flexible(1, 32)
        );
        assert_eq!(strategy.call_count(), 0); // No calculations yet
    }

    #[tokio::test]
    async fn test_batch_manager_calculate_no_reports() {
        let handle = RuntimeHandle::new();
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(2, 64));
        let manager = BatchManager::new(strategy.clone(), &handle);

        let req = manager.calculate_batch_size().await;
        assert_eq!(req, MorselSizeRequirement::Flexible(2, 64)); // Should return initial
        assert_eq!(strategy.call_count(), 0); // No reports, no calculations
    }

    #[tokio::test]
    async fn test_batch_manager_record_and_calculate() {
        let runtime = RuntimeHandle::new();
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(1, 16));
        let manager = BatchManager::new(strategy.clone(), &runtime);

        // Record some execution stats
        manager
            .record_execution_stats(Arc::new(MockRuntimeStats), 32, Duration::from_millis(100))
            .await;

        // Give background task time to process
        tokio::time::sleep(Duration::from_millis(10)).await;

        let req = manager.calculate_batch_size().await;
        assert_eq!(req, MorselSizeRequirement::Flexible(1, 10)); // First state transition
        assert_eq!(strategy.call_count(), 1);
    }

    #[tokio::test]
    async fn test_batch_manager_multiple_reports() {
        let runtime = RuntimeHandle::new();
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(1, 8));
        let manager = BatchManager::new(strategy.clone(), &runtime);

        // Record multiple stats
        manager
            .record_execution_stats(Arc::new(MockRuntimeStats), 10, Duration::from_millis(50))
            .await;
        manager
            .record_execution_stats(Arc::new(MockRuntimeStats), 20, Duration::from_millis(75))
            .await;

        tokio::time::sleep(Duration::from_millis(10)).await;

        let req = manager.calculate_batch_size().await;
        assert_eq!(req, MorselSizeRequirement::Flexible(5, 20)); // 2 reports processed
        assert_eq!(strategy.call_count(), 1);
    }

    #[tokio::test]
    async fn test_batch_manager_calculate_drains_reports() {
        let runtime = RuntimeHandle::new();
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(1, 4));
        let manager = BatchManager::new(strategy.clone(), &runtime);

        manager
            .record_execution_stats(Arc::new(MockRuntimeStats), 5, Duration::from_millis(25))
            .await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        let req1 = manager.calculate_batch_size().await;
        assert_eq!(req1, MorselSizeRequirement::Flexible(1, 10));

        // Second call should not trigger calculation since no new reports
        let req2 = manager.calculate_batch_size().await;
        assert_eq!(req2, MorselSizeRequirement::Flexible(1, 10)); // Same result
        assert_eq!(strategy.call_count(), 1); // Only called once
    }

    #[tokio::test]
    async fn test_batch_manager_accumulates_state() {
        let runtime = RuntimeHandle::new();
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(2, 8));
        let manager = BatchManager::new(strategy.clone(), &runtime);

        // First batch of reports
        manager
            .record_execution_stats(Arc::new(MockRuntimeStats), 10, Duration::from_millis(30))
            .await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let req1 = manager.calculate_batch_size().await;
        assert_eq!(req1, MorselSizeRequirement::Flexible(1, 10));

        // Second batch of reports
        manager
            .record_execution_stats(Arc::new(MockRuntimeStats), 15, Duration::from_millis(40))
            .await;
        manager
            .record_execution_stats(Arc::new(MockRuntimeStats), 20, Duration::from_millis(60))
            .await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let req2 = manager.calculate_batch_size().await;
        assert_eq!(req2, MorselSizeRequirement::Flexible(5, 20)); // 1 + 2 = 3 total reports

        assert_eq!(strategy.call_count(), 2);
    }

    #[tokio::test]
    async fn test_batch_manager_concurrent_access() {
        let runtime = RuntimeHandle::new();
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(1, 2));
        let manager = Arc::new(BatchManager::new(strategy.clone(), &runtime));

        // Spawn multiple tasks recording stats concurrently
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let manager = manager.clone();
                tokio::spawn(async move {
                    manager
                        .record_execution_stats(
                            Arc::new(MockRuntimeStats),
                            i,
                            Duration::from_millis(i as u64 * 10),
                        )
                        .await;
                })
            })
            .collect();

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(20)).await;

        let req = manager.calculate_batch_size().await;
        assert_eq!(req, MorselSizeRequirement::Flexible(10, 50)); // 10 reports processed
        assert_eq!(strategy.call_count(), 1);
    }

    #[tokio::test]
    async fn test_batch_manager_with_static_strategy() {
        let runtime = RuntimeHandle::new();
        let static_req = MorselSizeRequirement::Flexible(16, 128);
        let strategy = StaticBatchingStrategy::new(static_req);
        let manager = BatchManager::new(strategy, &runtime);

        assert_eq!(manager.initial_requirements(), static_req);

        // Even after recording stats, static strategy should return same requirement
        manager
            .record_execution_stats(Arc::new(MockRuntimeStats), 64, Duration::from_millis(200))
            .await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        let req = manager.calculate_batch_size().await;
        assert_eq!(req, static_req);
    }
}
