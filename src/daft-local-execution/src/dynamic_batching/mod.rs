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
    strategy: S,
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
