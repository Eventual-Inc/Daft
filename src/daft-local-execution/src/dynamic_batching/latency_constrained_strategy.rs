use std::{collections::VecDeque, num::NonZeroUsize, time::Duration};

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
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct LatencyConstrainedBatchingStrategy {
    /// Target maximum batch latency (𝐷SLA)
    pub target_batch_latency: Duration,
    /// Slack/tolerance around target latency for stability (εD).
    ///
    /// Prevents oscillation when latency hovers near the boundary.
    /// Typical value: 5-10% of target_batch_latency
    pub latency_tolerance: Duration,
    /// Step size (α) for adjusting search bounds when latency is out of range.
    ///
    /// Controls how aggressively the algorithm expands/contracts the search space.
    /// Larger values = faster adaptation but potentially more oscillation.
    pub step_size_alpha: usize,
    /// Correction factor (δ) for small nudges when inside latency range.
    ///
    /// When latency is within the target, this controls how much to explore
    /// the search space around the current batch size.
    /// Typical value: 5-10
    pub correction_delta: usize,
    /// Minimum allowed batch size (hard lower bound).
    ///
    /// Ensures we always process at least this many requests together.
    /// Typical value: 1
    pub b_min: usize,
    /// Maximum allowed batch size (hard upper bound).
    ///
    /// Prevents excessive memory usage or OOM errors.
    pub b_max: usize,
}

pub struct LatencyConstrainedBatchingState {
    /// Current batch size (b_t in the paper).
    ///
    /// The batch size currently being used for processing. This is typically
    /// the midpoint of the current search space [b_low, b_high].
    current_batch_size: usize,
    /// Lower bound of binary search space (b_low in Algorithm 2).
    ///
    /// The minimum batch size in the current search range. The algorithm
    /// contracts this bound upward when latency is acceptable, and expands
    /// it downward when latency is too high.
    b_low: usize,
    /// Upper bound of binary search space (b_high in Algorithm 2).
    ///
    /// The maximum batch size in the current search range. The algorithm
    /// expands this bound upward when latency is good, and contracts
    /// it downward when latency exceeds the target.
    b_high: usize,
    /// Rolling window of recent batch latencies for averaging.
    recent_latencies: VecDeque<Duration>,
    /// Rolling window of recent batch sizes for averaging.
    recent_batch_sizes: VecDeque<usize>,
}

impl LatencyConstrainedBatchingState {
    /// Window size for recent latencies and batch sizes.
    const WINDOW_SIZE: usize = 16;

    /// Get recent average latency (¯𝜏)
    /// and recent average batch size (¯𝑏)
    fn avg_batch_size_and_latency(&self) -> Option<(usize, Duration)> {
        // latencies and batch_sizes will always be of the same length
        if self.recent_latencies.is_empty() {
            None
        } else {
            let sum: Duration = self.recent_latencies.iter().sum();
            let avg_latency = sum / self.recent_latencies.len() as u32;
            let avg_batch_size =
                self.recent_batch_sizes.iter().sum::<usize>() / self.recent_batch_sizes.len();

            Some((avg_batch_size, avg_latency))
        }
    }
}

impl BatchingState for LatencyConstrainedBatchingState {
    fn record_execution_stat(
        &mut self,
        _stats: &dyn RuntimeStats,
        batch_size: usize,
        duration: Duration,
    ) {
        self.recent_latencies.push_back(duration);
        self.recent_batch_sizes.push_back(batch_size);
        if self.recent_latencies.len() > Self::WINDOW_SIZE {
            self.recent_latencies.pop_front();
            self.recent_batch_sizes.pop_front();
        }
    }
}

impl LatencyConstrainedBatchingState {
    pub fn new(initial_batch_size: usize, min: usize, max: usize) -> Self {
        Self {
            current_batch_size: initial_batch_size.max(1),
            b_low: min,
            b_high: max,
            recent_latencies: VecDeque::with_capacity(Self::WINDOW_SIZE),
            recent_batch_sizes: VecDeque::with_capacity(Self::WINDOW_SIZE),
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
        LatencyConstrainedBatchingState::new(self.b_min, self.b_min, 256)
    }

    fn initial_requirements(&self) -> MorselSizeRequirement {
        let default_morsel_size = daft_context::get_context()
            .execution_config()
            .default_morsel_size;
        let upper_bound = default_morsel_size.min(NonZeroUsize::new(256).unwrap());
        // start with a small initial requirement that matches our search space
        MorselSizeRequirement::Flexible(1, upper_bound)
    }

    fn calculate_new_requirements(&self, state: &mut Self::State) -> MorselSizeRequirement {
        // Get recent average latency ¯𝜏
        // Get recent average batch size ¯𝑏
        let Some((b, t)) = state.avg_batch_size_and_latency() else {
            return self.initial_requirements();
        };

        // 𝐷SLA
        let delta_sla = self.target_batch_latency;

        log::debug!(
            "[{}] 𝜏={}ms, {}ms±{}ms, batch_size={}, search=[{}, {}]",
            std::thread::current().name().unwrap_or("unknown"),
            t.as_millis(),
            delta_sla.as_millis(),
            self.latency_tolerance.as_millis(),
            b,
            state.b_low,
            state.b_high,
        );

        // Binary search adjustment - conservative expansion
        // if ¯𝜏 > 𝐷SLA + 𝜖D
        if t > delta_sla + self.latency_tolerance {
            // Latency too high, reduce search space
            log::debug!(
                "[{}] LATENCY TOO HIGH (𝜏={}ms > 𝐷SLA={}ms), contracting search space search=[{}, {}] b_t=({})",
                std::thread::current().name().unwrap_or("unknown"),
                t.as_millis(),
                (delta_sla + self.latency_tolerance).as_millis(),
                state.b_low,
                state.b_high,
                state.current_batch_size
            );
            // 𝑏high 𝑡 ← max{ ¯𝑏, 𝑏low 𝑡 −1 + 𝛼}
            state.b_high = usize::max(
                b,
                state
                    .b_low
                    .saturating_sub(1)
                    .saturating_add(self.step_size_alpha),
            );
            // 𝑏low 𝑡 ← max{𝑏low 𝑡 −1 − 𝛿, 𝐵min }
            state.b_low = usize::max(
                state
                    .b_low
                    .saturating_sub(1)
                    .saturating_sub(self.correction_delta),
                self.b_min,
            );

        // else if ¯𝜏 < 𝐷SLA − 𝜖D
        } else if t < delta_sla - self.latency_tolerance {
            // Latency good, expand search space
            log::debug!(
                "[{}] LATENCY GOOD (𝜏={}ms < 𝐷SLA={}ms), expanding search space",
                std::thread::current().name().unwrap_or("unknown"),
                t.as_millis(),
                (delta_sla - self.latency_tolerance).as_millis(),
            );
            // 𝑏low 𝑡 ← min{ ¯𝑏, 𝑏high 𝑡 −1 − 𝛼}
            state.b_low = usize::max(
                b,
                state
                    .b_high
                    .saturating_sub(1)
                    .saturating_sub(self.step_size_alpha),
            );
            // 𝑏high 𝑡 ← min{𝑏high 𝑡 −1 + 𝛿, 𝐵max }
            state.b_high = usize::min(
                self.b_max,
                state
                    .b_high
                    .saturating_sub(1)
                    .saturating_add(self.correction_delta),
            );
        } else {
            // Within range - tighten search around current point
            log::debug!(
                "[{}] WITHIN RANGE (𝜏={}ms in range), tightening search space",
                std::thread::current().name().unwrap_or("unknown"),
                t.as_millis(),
            );

            let tighten_amount = self.step_size_alpha.saturating_div(2);
            // 𝑏high 𝑡 ← min{ ¯𝑏 + ⌊𝛼/2⌋, 𝐵max }
            state.b_high = usize::min(b.saturating_add(tighten_amount), self.b_max);
            // 𝑏low 𝑡 ← max{ ¯𝑏 − ⌊𝛼/2⌋, 𝐵min }
            state.b_low = usize::max(b.saturating_sub(tighten_amount), self.b_min);
        }

        // Midpoint of search space
        // 𝑏𝑡 ← ⌊(𝑏low 𝑡 + 𝑏high 𝑡 )/2⌋
        state.current_batch_size = usize::midpoint(state.b_low, state.b_high);

        // We don't have context of the number of currently processing rows
        // so we leave out the last part of the equation `𝑏𝑡 ← min{max{𝑏𝑡 , 𝑁d𝑡 −1 }, 𝐵max }`
        // and instead just clip it to `𝑏𝑡 ← min{max{𝑏𝑡 , 𝐵min }, 𝐵max }`
        state.current_batch_size = state.current_batch_size.min(self.b_max).max(self.b_min);

        log::debug!(
            "[{}] new_search=[{}, {}], new_batch_size={}",
            std::thread::current().name().unwrap_or("unknown"),
            state.b_low,
            state.b_high,
            state.current_batch_size,
        );
        MorselSizeRequirement::Flexible(
            self.b_min,
            NonZeroUsize::new(state.current_batch_size).unwrap_or(NonZeroUsize::MIN),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use common_metrics::{Meter, ops::NodeInfo};

    use super::*;
    use crate::runtime_stats::RuntimeStats;

    struct MockRuntimeStats;
    impl RuntimeStats for MockRuntimeStats {
        fn new(_meter: &Meter, _node_info: &NodeInfo) -> Self {
            Self {}
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
        fn add_duration_us(&self, _cpu_us: u64) {
            unimplemented!()
        }
        fn add_bytes_in(&self, _bytes: u64) {}
        fn add_bytes_out(&self, _bytes: u64) {}
        fn increment_num_tasks(&self) {}
    }

    fn create_strategy() -> LatencyConstrainedBatchingStrategy {
        LatencyConstrainedBatchingStrategy {
            target_batch_latency: Duration::from_millis(100),
            latency_tolerance: Duration::from_millis(10),
            step_size_alpha: 20,
            correction_delta: 5,
            b_min: 1,
            b_max: 512,
        }
    }

    fn stats() -> Arc<dyn RuntimeStats> {
        Arc::new(MockRuntimeStats)
    }

    #[test]
    fn test_latency_state_initialization() {
        let strategy = create_strategy();
        let state = strategy.make_state();

        assert_eq!(state.current_batch_size, 1);
        assert_eq!(state.b_low, 1);
        assert_eq!(state.b_high, 256);
    }

    #[test]
    fn test_latency_too_high_contracts_search() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();
        state.current_batch_size = 100;
        state.b_low = 50;
        state.b_high = 200;

        // Latency = 150ms, target = 100ms + 10ms = 110ms tolerance
        state.record_execution_stat(stats().as_ref(), 100, Duration::from_millis(150));
        let _req = strategy.calculate_new_requirements(&mut state);

        // Should contract search space (search_high should be reduced)
        assert!(state.b_high < 200);
        assert_eq!(state.b_low, 44); // reduced by correction_delta
    }

    #[test]
    fn test_latency_good_expands_search() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();
        state.current_batch_size = 50;
        state.b_low = 40;
        state.b_high = 100;

        // Latency = 50ms, target = 100ms - 10ms = 90ms tolerance
        state.record_execution_stat(stats().as_ref(), 50, Duration::from_millis(50));

        let _req = strategy.calculate_new_requirements(&mut state);

        // Should expand search space
        assert_eq!(state.b_low, 79);
        assert_eq!(state.b_high, 104);
    }

    #[test]
    fn test_latency_within_range_tightens_search() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();
        state.b_low = 40;
        state.b_high = 120;

        // Latency = 100ms, exactly at target

        state.record_execution_stat(stats().as_ref(), 80, Duration::from_millis(100));

        let _req = strategy.calculate_new_requirements(&mut state);

        // Should tighten around current point
        let _tighten_amount = (strategy.step_size_alpha / 2).max(1); // 10
        assert_eq!(state.b_high, 90); // 80 + 10
        assert_eq!(state.b_low, 70); // 80 - 10
    }

    #[test]
    fn test_latency_respects_min_max_bounds() {
        let strategy = LatencyConstrainedBatchingStrategy {
            target_batch_latency: Duration::from_millis(100),
            latency_tolerance: Duration::from_millis(10),
            step_size_alpha: 20,
            correction_delta: 5,
            b_min: 10,
            b_max: 50,
        };

        let mut state = strategy.make_state();
        state.record_execution_stat(stats().as_ref(), 5, Duration::from_millis(50));

        let _req = strategy.calculate_new_requirements(&mut state);

        assert!(state.current_batch_size >= strategy.b_min);
        assert!(state.current_batch_size <= strategy.b_max);
    }

    #[test]
    #[cfg(feature = "python")]
    fn test_latency_empty_batch_handling() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();

        let _req = strategy.calculate_new_requirements(&mut state);

        // Should handle gracefully without panicking
        assert!(state.current_batch_size >= strategy.b_min);
    }

    #[test]
    fn test_latency_multiple_batch_entries() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();

        state.record_execution_stat(stats().as_ref(), 50, Duration::from_millis(80));
        state.record_execution_stat(stats().as_ref(), 60, Duration::from_millis(120));
        state.record_execution_stat(stats().as_ref(), 70, Duration::from_millis(100));

        let _req = strategy.calculate_new_requirements(&mut state);

        // Should handle multiple entries (avg latency = 100ms, avg batch = 60)
        assert!(state.current_batch_size > 0);
    }

    #[test]
    fn test_latency_search_space_convergence() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();

        // Simulate multiple iterations with good latency
        for _ in 0..5 {
            state.record_execution_stat(
                stats().as_ref(),
                state.current_batch_size,
                Duration::from_millis(95),
            );
            strategy.calculate_new_requirements(&mut state);
        }

        // Search space should converge (high - low should be small)
        let search_space = state.b_high.saturating_sub(state.b_low);
        assert!(search_space > 0); // Should still have some space to search
    }

    #[test]
    fn test_latency_max_batch_size_constraint() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();
        state.b_high = strategy.b_max + 100;

        state.record_execution_stat(stats().as_ref(), 50, Duration::from_millis(50));
        let _req = strategy.calculate_new_requirements(&mut state);
        assert!(state.b_high <= strategy.b_max);
        assert!(state.current_batch_size <= strategy.b_max);
    }
}
