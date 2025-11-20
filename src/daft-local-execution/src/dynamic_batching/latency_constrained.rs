use std::{collections::VecDeque, time::Duration};

use crate::{dynamic_batching::BatchingStrategy, runtime_stats::RuntimeStats};

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

impl LatencyConstrainedBatchingStrategy {
    #[allow(dead_code)]
    pub fn new(
        target_decoding_latency: Duration,
        latency_slack: Duration,
        step_size_alpha: usize,
        correction_delta: usize,
        min_batch_size: usize,
        max_batch_size: usize,
    ) -> Self {
        Self {
            target_batch_latency: target_decoding_latency,
            latency_tolerance: latency_slack,
            step_size_alpha,
            correction_delta,
            min_batch_size,
            max_batch_size,
        }
    }
    pub fn with_step_size(mut self, step_size: usize) -> Self {
        self.step_size_alpha = step_size;
        self
    }
}

impl Default for LatencyConstrainedBatchingStrategy {
    fn default() -> Self {
        Self {
            target_batch_latency: Duration::from_millis(2000),
            latency_tolerance: Duration::from_millis(100),
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
    recent_latencies: VecDeque<Duration>,
    recent_batch_sizes: VecDeque<usize>,
}

impl LatencyConstrainedBatchingState {
    pub fn new(initial_batch_size: usize, min: usize, max: usize) -> Self {
        Self {
            current_batch_size: initial_batch_size,
            search_low: min,
            search_high: max,
            recent_latencies: VecDeque::with_capacity(24),
            recent_batch_sizes: VecDeque::with_capacity(24),
        }
    }
}

impl BatchingStrategy for LatencyConstrainedBatchingStrategy {
    type State = LatencyConstrainedBatchingState;

    fn make_state(&self) -> Self::State {
        log::debug!(
            "[{}] Initializing state with search space [1, 256]",
            std::thread::current().name().unwrap_or("unknown")
        );
        // start off with a small search space (1 - 256)
        LatencyConstrainedBatchingState::new(1, self.min_batch_size, 256)
    }

    fn adjust_batch_size(
        &self,
        state: &mut Self::State,
        _runtime: &dyn RuntimeStats,
        duration: Duration,
    ) -> usize {
        // Record metrics
        state.recent_latencies.push_back(duration);
        state.recent_batch_sizes.push_back(state.current_batch_size);

        if state.recent_latencies.len() > 24 {
            state.recent_latencies.pop_front();
            state.recent_batch_sizes.pop_front();
        }

        // Calculate average latency
        let avg_latency = if !state.recent_latencies.is_empty() {
            let total: Duration = state.recent_latencies.iter().sum();
            total / state.recent_latencies.len() as u32
        } else {
            duration
        };

        let avg_batch_size = if !state.recent_batch_sizes.is_empty() {
            state.recent_batch_sizes.iter().sum::<usize>() / state.recent_batch_sizes.len()
        } else {
            state.current_batch_size
        };

        let search_space = state.search_high.saturating_sub(state.search_low);

        log::debug!(
            "[{}] observed_latency={}ms, avg_latency={}ms, target={}ms±{}ms, avg_batch_size={}, search=[{}, {}], search_space={}",
            std::thread::current().name().unwrap_or("unknown"),
            duration.as_millis(),
            avg_latency.as_millis(),
            self.target_batch_latency.as_millis(),
            self.latency_tolerance.as_millis(),
            avg_batch_size,
            state.search_low,
            state.search_high,
            search_space,
        );

        // Binary search adjustment - conservative expansion
        if avg_latency > self.target_batch_latency + self.latency_tolerance {
            // Latency too high, reduce search space
            log::debug!(
                "[{}] LATENCY TOO HIGH ({}ms > {}ms), contracting search space",
                std::thread::current().name().unwrap_or("unknown"),
                avg_latency.as_millis(),
                (self.target_batch_latency + self.latency_tolerance).as_millis(),
            );

            state.search_high = (avg_batch_size / 2).max(state.search_low + 1);
            state.search_low = state.search_low.saturating_sub(self.correction_delta);
        } else if avg_latency < self.target_batch_latency - self.latency_tolerance {
            // Latency good, expand search space
            log::debug!(
                "[{}] LATENCY GOOD ({}ms < {}ms), expanding search space",
                std::thread::current().name().unwrap_or("unknown"),
                avg_latency.as_millis(),
                (self.target_batch_latency - self.latency_tolerance).as_millis(),
            );

            state.search_low = avg_batch_size.max(state.search_low);
            state.search_high = state
                .search_high
                .saturating_add(self.step_size_alpha)
                .min(self.max_batch_size);
        } else {
            // Within range - tighten search around current point
            log::debug!(
                "[{}] WITHIN RANGE ({}ms in range), tightening search space",
                std::thread::current().name().unwrap_or("unknown"),
                avg_latency.as_millis(),
            );

            let tighten_amount = (self.step_size_alpha / 2).max(1);

            state.search_high = avg_batch_size
                .saturating_add(tighten_amount)
                .min(self.max_batch_size);
            state.search_low = avg_batch_size
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

    fn current_batch_size(&self, state: &Self::State) -> usize {
        state.current_batch_size
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::runtime_stats::DefaultRuntimeStats;

    #[test]
    fn test_latency_constrained_initial_state() {
        let batching = LatencyConstrainedBatchingStrategy::default();
        let state = batching.make_state();

        assert_eq!(state.current_batch_size, 1);
        assert_eq!(state.search_low, 1);
        assert_eq!(state.search_high, 1024);
        assert!(state.recent_latencies.is_empty());
        assert!(state.recent_batch_sizes.is_empty());
    }

    #[test]
    fn test_latency_too_high_contracts_search_space() {
        let batching = LatencyConstrainedBatchingStrategy::default();
        let mut state = batching.make_state();
        let stats = DefaultRuntimeStats::new(0);
        let initial_search_space = state.search_high - state.search_low;

        // Apply high latency multiple times
        for _ in 0..5 {
            batching.adjust_batch_size(&mut state, &stats, Duration::from_millis(10000));
        }

        let final_search_space = state.search_high.saturating_sub(state.search_low);
        // Need multiple iterations for contraction to be visible
        for _ in 0..5 {
            batching.adjust_batch_size(&mut state, &stats, Duration::from_millis(10000));
        }
        // Search space should shrink
        assert!(final_search_space < initial_search_space);
        // And current batch size should be low
        assert!(state.current_batch_size < 256);
    }

    #[test]
    fn test_latency_too_low_expands_search_space() {
        let batching = LatencyConstrainedBatchingStrategy::default();
        let mut state = batching.make_state();
        let stats = DefaultRuntimeStats::new(0);

        // Simulate low latency (well below target - slack)
        let low_latency = Duration::from_millis(100);
        let new_batch_size = batching.adjust_batch_size(&mut state, &stats, low_latency);

        // Search space should expand
        assert!(state.search_high >= 1024);
        assert!(new_batch_size > 1);
    }

    #[test]
    fn test_latency_within_sla_tightens_search() {
        let batching = LatencyConstrainedBatchingStrategy::default();
        let mut state = batching.make_state();
        let stats = DefaultRuntimeStats::new(0);

        // Simulate latency within SLA (target ± slack)
        let target_latency = Duration::from_millis(5000);
        let within_sla_latency = target_latency + Duration::from_millis(50); // Within slack of 100ms

        batching.adjust_batch_size(&mut state, &stats, within_sla_latency);

        // Search space should tighten around current batch size
        let search_space = state.search_high.saturating_sub(state.search_low);
        assert!(search_space < 1024);
    }

    #[test]
    fn test_latency_convergence_to_optimal_batch_size() {
        let batching = LatencyConstrainedBatchingStrategy::default();
        let mut state = batching.make_state();
        let stats = DefaultRuntimeStats::new(0);

        // Simulate gradual approach to target latency
        for i in 0..20 {
            let latency = if i < 5 {
                Duration::from_millis(100)
            } else if i < 15 {
                Duration::from_millis(1000 + (i as u64 - 5) * 300)
            } else {
                Duration::from_millis(5000)
            };

            batching.adjust_batch_size(&mut state, &stats, latency);
        }

        // Should converge to some stable batch size between min and max
        assert!(state.current_batch_size >= batching.min_batch_size);
        assert!(state.current_batch_size <= batching.max_batch_size);

        // Should have stopped expanding/contracting wildly
        // Last few batch sizes should be close to each other
        let variance = state
            .recent_batch_sizes
            .iter()
            .map(|&x| x as i64)
            .collect::<Vec<_>>();
        if variance.len() >= 3 {
            let max_diff = (variance[variance.len() - 1] - variance[variance.len() - 3]).abs();
            assert!(max_diff < 5000); // Reasonable convergence
        }
    }

    #[test]
    fn test_latency_respects_min_batch_size() {
        let batching = LatencyConstrainedBatchingStrategy::new(
            Duration::from_millis(5000),
            Duration::from_millis(100),
            1024,
            128,
            10,
            256,
        );
        let mut state = batching.make_state();
        let stats = DefaultRuntimeStats::new(0);

        // Simulate high latency to force contraction
        for _ in 0..10 {
            batching.adjust_batch_size(&mut state, &stats, Duration::from_secs(10));
        }

        // Should never go below min_batch_size
        assert!(state.current_batch_size >= 10);
    }

    #[test]
    fn test_latency_respects_max_batch_size() {
        let batching = LatencyConstrainedBatchingStrategy::default();
        let mut state = batching.make_state();
        let stats = DefaultRuntimeStats::new(0);

        // Simulate very low latency to force expansion
        for _ in 0..50 {
            batching.adjust_batch_size(&mut state, &stats, Duration::from_millis(10));
        }

        // Should never exceed max_batch_size
        assert!(state.current_batch_size <= batching.max_batch_size);
    }

    #[test]
    fn test_latency_history_window_size_limited() {
        let batching = LatencyConstrainedBatchingStrategy::default();
        let mut state = batching.make_state();
        let stats = DefaultRuntimeStats::new(0);

        // Fill history beyond window size
        for i in 0..20 {
            let latency = Duration::from_millis(100 + (i as u64 * 10));
            batching.adjust_batch_size(&mut state, &stats, latency);
        }

        // Should only keep last 10
        assert_eq!(state.recent_latencies.len(), 10);
        assert_eq!(state.recent_batch_sizes.len(), 10);
    }

    #[test]
    fn test_latency_average_latency_calculation() {
        let batching = LatencyConstrainedBatchingStrategy::default();
        let mut state = batching.make_state();
        let stats = DefaultRuntimeStats::new(0);

        // Add specific latencies
        batching.adjust_batch_size(&mut state, &stats, Duration::from_millis(100));
        batching.adjust_batch_size(&mut state, &stats, Duration::from_millis(200));
        batching.adjust_batch_size(&mut state, &stats, Duration::from_millis(300));

        // Average should be 200ms
        let total: Duration = state.recent_latencies.iter().sum();
        let avg = total / state.recent_latencies.len() as u32;
        assert_eq!(avg, Duration::from_millis(200));
    }

    #[test]
    fn test_latency_no_oscillation_when_stable() {
        let batching = LatencyConstrainedBatchingStrategy::default();
        let mut state = batching.make_state();
        let stats = DefaultRuntimeStats::new(0);

        // Simulate stable latency within SLA
        let stable_latency = Duration::from_millis(5000);
        let mut batch_sizes = Vec::new();

        for _ in 0..10 {
            let batch_size = batching.adjust_batch_size(&mut state, &stats, stable_latency);
            batch_sizes.push(batch_size);
        }

        // Batch sizes should stabilize (not wildly jumping)
        let last_three = &batch_sizes[batch_sizes.len() - 3..];
        let variance = last_three.iter().map(|&x| x as i32).collect::<Vec<_>>();
        let max_diff = (variance[2] - variance[0]).abs();
        assert!(max_diff < 100); // Should be relatively stable
    }

    #[test]
    fn test_latency_custom_parameters() {
        let batching = LatencyConstrainedBatchingStrategy::new(
            Duration::from_millis(1000),
            Duration::from_millis(50),
            256,
            32,
            5,
            512,
        );

        assert_eq!(batching.target_batch_latency, Duration::from_millis(1000));
        assert_eq!(batching.latency_tolerance, Duration::from_millis(50));
        assert_eq!(batching.step_size_alpha, 256);
        assert_eq!(batching.correction_delta, 32);
        assert_eq!(batching.min_batch_size, 5);
        assert_eq!(batching.max_batch_size, 512);
    }
}
