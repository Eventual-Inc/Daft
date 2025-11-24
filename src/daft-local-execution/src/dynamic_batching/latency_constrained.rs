use std::time::Duration;

use crate::{
    dynamic_batching::{BatchingState, BatchingStrategy},
    pipeline::MorselSizeRequirement,
    runtime_stats::RuntimeStats,
};

/// Latency-constrained dynamic batching
///
/// This implementation adapts Algorithm 2 from:
/// "Optimizing LLM Inference Throughput via Memory-aware and SLA-constrained Dynamic Batching"
/// Bowen Pang, Kai Li, Feifan Wang (2025)
/// https://arxiv.org/abs/2503.05248
///
/// # Problem Statement
///
/// There is a fundamental tradeoff between:
/// - **Throughput**: Larger batch sizes increase parallelism and tokens/second
/// - **Latency**: Larger batch sizes increase batch processing time
/// - **Memory**: Larger batch sizes increase memory usage
///
/// This algorithm optimizes batch size to maximize throughput while respecting an
/// upper limit on latency.
///
/// # How It Works
///
/// The algorithm uses **binary search** to find the largest batch size that keeps
/// batch latency within the latency constraint. It maintains a search range [b_low, b_high]
/// and adjusts it based on observed latencies:
///
/// - If latency exceeds target: contract search space downward
/// - If latency is well below target: expand search space upward
/// - If latency is within target: tighten search around current point
///
/// This converges to an optimal batch size with minimal oscillation.
///
/// # Paper References
///
/// - Section III.B: "Solution with target constraint"
/// - Algorithm 2: "target constrained dynamic batching"
/// - Figure 3: Shows relationship between batch size, throughput, and decoding time
/// - Equation (3): target constraint formulation: D(b_t) - D_SLA ≤ ε_D
///
/// # Example
///
/// ```rust,ignore
/// use std::time::Duration;
///
/// // Target: 100ms max batch latency
/// let batching = LatencyConstrainedBatching::new(
///     Duration::from_millis(100),  // target_batch_latency
///     Duration::from_millis(10),   // latency_tolerance
///     50,                          // step_size_alpha
///     5,                           // correction_delta
///     1,                           // min_batch_size
///     256,                         // max_batch_size
/// );
///
/// let mut state = batching.make_state();
///
/// // After each batch processes:
/// let new_batch_size = batching.adjust_batch_size(
///     &mut state,
///     measured_latency,
///     None,
/// );
/// ```
#[derive(Clone)]
pub struct LatencyConstrainedBatchingStrategy {
    /// Target maximum batch latency
    ///
    /// From paper Equation (3): D(b_t) ≤ D_SLA
    target_batch_latency: Duration,
    /// Slack/tolerance around target latency for stability.
    ///
    /// Prevents oscillation when latency hovers near the boundary.
    /// Typical value: 5-10% of target_batch_latency
    latency_tolerance: Duration,
    /// Step size (α) for adjusting search bounds when latency is out of range.
    ///
    /// Controls how aggressively the algorithm expands/contracts the search space.
    /// Larger values = faster adaptation but potentially more oscillation.
    step_size_alpha: usize,
    /// Correction factor (δ) for small nudges when inside latency range.
    ///
    /// When latency is within the target, this controls how much to explore
    /// the search space around the current batch size.
    /// Typical value: 5-10
    correction_delta: usize,
    /// Minimum allowed batch size (hard lower bound).
    ///
    /// Ensures we always process at least this many requests together.
    /// Typical value: 1
    min_batch_size: usize,
    /// Maximum allowed batch size (hard upper bound).
    ///
    /// Prevents excessive memory usage or OOM errors.
    /// From paper: corresponds to B_max constraint
    max_batch_size: usize,
}

#[allow(dead_code)]
impl LatencyConstrainedBatchingStrategy {
    pub fn new(
        target_decoding_latency: Duration,
        latency_slack: Duration,
        step_size_alpha: usize,
        correction_delta: usize,
        morsel_size_req: Option<&MorselSizeRequirement>,
    ) -> Self {
        let (min, max) = match morsel_size_req {
            Some(MorselSizeRequirement::Strict(v)) => (0, *v),
            Some(MorselSizeRequirement::Flexible(min, max)) => (*min, *max),
            None => (0, 128 * 1024),
        };
        Self {
            target_batch_latency: target_decoding_latency,
            latency_tolerance: latency_slack,
            step_size_alpha,
            correction_delta,
            min_batch_size: min,
            max_batch_size: max,
        }
    }
    pub fn with_target_batch_latency(mut self, target_batch_latency: Duration) -> Self {
        self.target_batch_latency = target_batch_latency;
        self
    }
    pub fn with_latency_tolerance(mut self, latency_tolerance: Duration) -> Self {
        self.latency_tolerance = latency_tolerance;
        self
    }
    pub fn with_correction_delta(mut self, correction_delta: usize) -> Self {
        self.correction_delta = correction_delta;
        self
    }

    pub fn with_step_size(mut self, step_size: usize) -> Self {
        self.step_size_alpha = step_size;
        self
    }
}

impl Default for LatencyConstrainedBatchingStrategy {
    fn default() -> Self {
        Self {
            target_batch_latency: Duration::from_millis(5000),
            latency_tolerance: Duration::from_millis(1000),
            step_size_alpha: 1024,
            correction_delta: 10,
            min_batch_size: 1,
            max_batch_size: 128 * 1024,
        }
    }
}

pub struct LatencyConstrainedBatchingState {
    current_batch_size: usize,
    search_low: usize,
    search_high: usize,
}

impl LatencyConstrainedBatchingState {
    pub fn new(initial_batch_size: usize, min: usize, max: usize) -> Self {
        Self {
            current_batch_size: initial_batch_size,
            search_low: min,
            search_high: max,
        }
    }
}
impl BatchingState for LatencyConstrainedBatchingState {}

impl BatchingStrategy for LatencyConstrainedBatchingStrategy {
    type State = LatencyConstrainedBatchingState;

    fn make_state(&self) -> Self::State {
        log::debug!(
            "[{}] Initializing state with search space [1, 256]",
            std::thread::current().name().unwrap_or("unknown")
        );
        // start off with a small search space (1 - 256)
        LatencyConstrainedBatchingState::new(self.min_batch_size, self.min_batch_size, 256)
    }

    fn initial_batch_size(&self) -> usize {
        self.min_batch_size
    }

    fn calculate_new_batch_size(
        &self,
        state: &mut Self::State,
        batch: Vec<(std::sync::Arc<dyn RuntimeStats>, usize, Duration)>,
    ) -> usize {
        let latency = avg_latency(batch.iter().map(|(_, _, latency)| *latency));
        let batch_size = avg_batch_size(batch.iter().map(|(_, batch_size, _)| *batch_size));
        let search_space = state.search_high.saturating_sub(state.search_low);

        log::debug!(
            "[{}] observed_latency={}ms, target={}ms±{}ms, batch_size={}, search=[{}, {}], search_space={}",
            std::thread::current().name().unwrap_or("unknown"),
            latency.as_millis(),
            self.target_batch_latency.as_millis(),
            self.latency_tolerance.as_millis(),
            batch_size,
            state.search_low,
            state.search_high,
            search_space,
        );

        // Binary search adjustment - conservative expansion
        if latency > self.target_batch_latency + self.latency_tolerance {
            // Latency too high, reduce search space
            log::debug!(
                "[{}] LATENCY TOO HIGH ({}ms > {}ms), contracting search space",
                std::thread::current().name().unwrap_or("unknown"),
                latency.as_millis(),
                (self.target_batch_latency + self.latency_tolerance).as_millis(),
            );

            state.search_high = (batch_size / 2).max(state.search_low + 1);
            state.search_low = state.search_low.saturating_sub(self.correction_delta);
        } else if latency < self.target_batch_latency - self.latency_tolerance {
            // Latency good, expand search space
            log::debug!(
                "[{}] LATENCY GOOD ({}ms < {}ms), expanding search space",
                std::thread::current().name().unwrap_or("unknown"),
                latency.as_millis(),
                (self.target_batch_latency - self.latency_tolerance).as_millis(),
            );

            state.search_low = batch_size.max(state.search_low);
            state.search_high = state
                .search_high
                .saturating_add(self.step_size_alpha)
                .min(self.max_batch_size);
        } else {
            // Within range - tighten search around current point
            log::debug!(
                "[{}] WITHIN RANGE ({}ms in range), tightening search space",
                std::thread::current().name().unwrap_or("unknown"),
                latency.as_millis(),
            );

            let tighten_amount = (self.step_size_alpha / 2).max(1);

            state.search_high = batch_size
                .saturating_add(tighten_amount)
                .min(self.max_batch_size);
            state.search_low = batch_size
                .saturating_sub(tighten_amount)
                .max(self.min_batch_size);
        }

        // Midpoint of search space
        state.current_batch_size = usize::midpoint(state.search_low, state.search_high);
        state.current_batch_size = state
            .current_batch_size
            .max(self.min_batch_size)
            .min(self.max_batch_size);

        log::debug!(
            "[{}] new_search=[{}, {}], new_batch_size={}",
            std::thread::current().name().unwrap_or("unknown"),
            state.search_low,
            state.search_high,
            state.current_batch_size,
        );

        state.current_batch_size
    }
}

fn avg_latency(latencies: impl Iterator<Item = Duration>) -> Duration {
    let latencies = latencies.collect::<Vec<_>>();
    if latencies.is_empty() {
        Duration::from_millis(0)
    } else {
        let sum: Duration = latencies.iter().sum();
        sum / latencies.len() as u32
    }
}

fn avg_batch_size(batch_sizes: impl Iterator<Item = usize>) -> usize {
    let batch_sizes = batch_sizes.collect::<Vec<_>>();
    if batch_sizes.is_empty() {
        0
    } else {
        batch_sizes.iter().sum::<usize>() / batch_sizes.len()
    }
}
