mod aimd;
use std::time::Duration;

pub use aimd::*;

use crate::runtime_stats::RuntimeStats;

/// Trait for algorithms that dynamically adjust batch sizes based on execution performance.
///
/// Dynamic batching is a technique used to optimize throughput and latency by adjusting
/// the number of items processed together in a batch based on runtime performance metrics.
/// Different algorithms use various strategies to find the optimal batch size.
///
///
/// # Examples
///
/// ```rust,ignore
/// let algorithm = AimdBatching::default();
/// let mut state = algorithm.make_state();
/// let rt_stats = DefaultRuntimeStats::default();
///
/// // After processing a batch that took 100ms
/// let new_batch_size = algorithm.adjust_batch_size(&mut state, &rt_stats, Duration::from_millis(100));
/// ```
pub trait DynamicBatchingAlgorithm: Send + Sync {
    type State: Send + Sync + Unpin;
    fn make_state(&self) -> Self::State;
    /// adjust the batch size based on runtime performance metrics
    fn adjust_batch_size(
        &self,
        state: &mut Self::State,
        _runtime_stats: &dyn RuntimeStats,
        current_run_duration: Duration,
    ) -> usize;
    fn current_batch_size(&self, state: &Self::State) -> usize;
}
