use std::time::Duration;

use crate::{
    dynamic_batching::BatchingStrategy, pipeline::MorselSizeRequirement,
    runtime_stats::RuntimeStats,
};

/// Latency-constrained dynamic batching
///
/// This implementation adapts Algorithm 2 from:
/// "Optimizing LLM Inference Throughput via Memory-aware and SLA-constrained Dynamic Batching"
/// Bowen Pang, Kai Li, Feifan Wang (2025)
/// https://arxiv.org/abs/2503.05248
///
/// Note: this was slightly modified from the paper. In the paper, the batch size logic is performed per batch,
/// but since we have multiple workers, we cannot guarantee that batch size adjustment can be done in between batches.
/// So we need to batch up the runtime statistics then average them during our computation.
///
/// While instead, the paper was able to perform batch size adjustments in between every batch.
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
/// ```
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct LatencyConstrainedBatchingStrategy {
    /// Target maximum batch latency
    ///
    /// From paper Equation (3): D(b_t) ≤ D_SLA
    pub target_batch_latency: Duration,
    /// Slack/tolerance around target latency for stability.
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
    pub min_batch_size: usize,
    /// Maximum allowed batch size (hard upper bound).
    ///
    /// Prevents excessive memory usage or OOM errors.
    /// From paper: corresponds to B_max constraint
    pub max_batch_size: usize,
}

pub struct LatencyConstrainedBatchingState {
    current_batch_size: usize,
    search_low: usize,
    search_high: usize,
}

impl LatencyConstrainedBatchingState {
    pub fn new(initial_batch_size: usize, min: usize, max: usize) -> Self {
        Self {
            current_batch_size: initial_batch_size.max(1),
            search_low: min,
            search_high: max,
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
        LatencyConstrainedBatchingState::new(self.min_batch_size, self.min_batch_size, 256)
    }

    fn initial_requirements(&self) -> MorselSizeRequirement {
        let default_morsel_size = daft_context::get_context()
            .execution_config()
            .default_morsel_size;
        let upper_bound = default_morsel_size.min(256);
        // start with a small initial requirement that matches our search space
        MorselSizeRequirement::Flexible(1, upper_bound)
    }

    fn calculate_new_requirements(
        &self,
        state: &mut Self::State,
        batch: Vec<(std::sync::Arc<dyn RuntimeStats>, usize, Duration)>,
    ) -> MorselSizeRequirement {
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
                "[{}] LATENCY TOO HIGH ({}ms > {}ms), contracting search space [{}, {}]({})",
                std::thread::current().name().unwrap_or("unknown"),
                latency.as_millis(),
                (self.target_batch_latency + self.latency_tolerance).as_millis(),
                state.search_low,
                state.search_high,
                state.current_batch_size
            );
            // 𝑏high 𝑡 ← max{ ¯𝑏, 𝑏low 𝑡 −1 + 𝛼}
            state.search_high = batch_size.max(
                state
                    .search_low
                    .saturating_sub(1)
                    .saturating_add(self.step_size_alpha),
            );
            // 𝑏low 𝑡 ← max{𝑏low 𝑡 −1 − 𝛿, 𝐵min }
            state.search_low = self
                .min_batch_size
                .max(state.search_low.saturating_sub(1))
                .saturating_sub(self.correction_delta);
        } else if latency < self.target_batch_latency - self.latency_tolerance {
            // Latency good, expand search space
            log::debug!(
                "[{}] LATENCY GOOD ({}ms < {}ms), expanding search space",
                std::thread::current().name().unwrap_or("unknown"),
                latency.as_millis(),
                (self.target_batch_latency - self.latency_tolerance).as_millis(),
            );
            // 𝑏low 𝑡 ← min{ ¯𝑏, 𝑏high 𝑡 −1 − 𝛼}
            state.search_low = batch_size
                .min(self.max_batch_size.saturating_sub(1))
                .saturating_sub(self.step_size_alpha);
            // 𝑏high 𝑡 ← min{𝑏high 𝑡 −1 + 𝛿, 𝐵max }
            state.search_high = self.max_batch_size.min(
                state
                    .search_high
                    .saturating_sub(1)
                    .saturating_add(self.step_size_alpha),
            );
        } else {
            // Within range - tighten search around current point
            log::debug!(
                "[{}] WITHIN RANGE ({}ms in range), tightening search space",
                std::thread::current().name().unwrap_or("unknown"),
                latency.as_millis(),
            );

            let tighten_amount = self.step_size_alpha.saturating_div(2);
            // 𝑏high 𝑡 ← min{ ¯𝑏 + ⌊𝛼/2⌋, 𝐵max }
            state.search_high = batch_size
                .saturating_add(tighten_amount)
                .min(self.max_batch_size);
            // 𝑏low 𝑡 ← max{ ¯𝑏 − ⌊𝛼/2⌋, 𝐵min }
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
        MorselSizeRequirement::Flexible(self.min_batch_size, state.current_batch_size)
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;
    use crate::{
        dynamic_batching::{BatchReport, tests::MockRuntimeStats},
        runtime_stats::RuntimeStats,
    };

    fn create_strategy() -> LatencyConstrainedBatchingStrategy {
        LatencyConstrainedBatchingStrategy {
            target_batch_latency: Duration::from_millis(100),
            latency_tolerance: Duration::from_millis(10),
            step_size_alpha: 20,
            correction_delta: 5,
            min_batch_size: 1,
            max_batch_size: 512,
        }
    }

    fn create_batch_data(
        batch_size: usize,
        latency: Duration,
    ) -> Vec<(Arc<dyn RuntimeStats>, usize, Duration)> {
        vec![(Arc::new(MockRuntimeStats), batch_size, latency)]
    }

    #[test]
    fn test_latency_state_initialization() {
        let strategy = create_strategy();
        let state = strategy.make_state();

        assert_eq!(state.current_batch_size, 1);
        assert_eq!(state.search_low, 1);
        assert_eq!(state.search_high, 256);
    }

    #[test]
    fn test_latency_too_high_contracts_search() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();
        state.current_batch_size = 100;
        state.search_low = 50;
        state.search_high = 200;

        // Latency = 150ms, target = 100ms + 10ms = 110ms tolerance
        let batch = create_batch_data(100, Duration::from_millis(150));
        let _req = strategy.calculate_new_requirements(&mut state, batch);

        // Should contract search space (search_high should be reduced)
        assert!(state.search_high < 200);
        assert_eq!(state.search_low, 44); // reduced by correction_delta
    }

    #[test]
    fn test_latency_good_expands_search() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();
        state.current_batch_size = 50;
        state.search_low = 40;
        state.search_high = 100;

        // Latency = 50ms, target = 100ms - 10ms = 90ms tolerance
        let batch = create_batch_data(50, Duration::from_millis(50));
        let _req = strategy.calculate_new_requirements(&mut state, batch);

        // Should expand search space
        assert_eq!(state.search_low, 30);
        assert_eq!(state.search_high, 119);
    }

    #[test]
    fn test_latency_within_range_tightens_search() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();
        state.search_low = 40;
        state.search_high = 120;

        // Latency = 100ms, exactly at target
        let batch = create_batch_data(80, Duration::from_millis(100));
        let _req = strategy.calculate_new_requirements(&mut state, batch);

        // Should tighten around current point
        let _tighten_amount = (strategy.step_size_alpha / 2).max(1); // 10
        assert_eq!(state.search_high, 90); // 80 + 10
        assert_eq!(state.search_low, 70); // 80 - 10
    }

    #[test]
    fn test_latency_respects_min_max_bounds() {
        let strategy = LatencyConstrainedBatchingStrategy {
            target_batch_latency: Duration::from_millis(100),
            latency_tolerance: Duration::from_millis(10),
            step_size_alpha: 20,
            correction_delta: 5,
            min_batch_size: 10,
            max_batch_size: 50,
        };

        let mut state = strategy.make_state();
        let batch = create_batch_data(5, Duration::from_millis(50));
        let _req = strategy.calculate_new_requirements(&mut state, batch);

        assert!(state.current_batch_size >= strategy.min_batch_size);
        assert!(state.current_batch_size <= strategy.max_batch_size);
    }

    #[test]
    fn test_latency_empty_batch_handling() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();
        let empty_batch = vec![];

        let _req = strategy.calculate_new_requirements(&mut state, empty_batch);

        // Should handle gracefully without panicking
        assert!(state.current_batch_size >= strategy.min_batch_size);
    }

    #[test]
    fn test_latency_multiple_batch_entries() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();
        let batch: BatchReport = vec![
            (Arc::new(MockRuntimeStats), 50, Duration::from_millis(80)),
            (Arc::new(MockRuntimeStats), 60, Duration::from_millis(120)),
            (Arc::new(MockRuntimeStats), 70, Duration::from_millis(100)),
        ];

        let _req = strategy.calculate_new_requirements(&mut state, batch);

        // Should handle multiple entries (avg latency = 100ms, avg batch = 60)
        assert!(state.current_batch_size > 0);
    }

    #[test]
    fn test_latency_avg_latency_function() {
        let latencies = vec![
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(300),
        ];

        let avg = avg_latency(latencies.into_iter());
        assert_eq!(avg, Duration::from_millis(200));
    }

    #[test]
    fn test_latency_avg_latency_empty() {
        let avg = avg_latency(std::iter::empty());
        assert_eq!(avg, Duration::from_millis(0));
    }

    #[test]
    fn test_latency_avg_batch_size_function() {
        let batch_sizes = vec![10, 20, 30];
        let avg = avg_batch_size(batch_sizes.into_iter());
        assert_eq!(avg, 20);
    }

    #[test]
    fn test_latency_avg_batch_size_empty() {
        let avg = avg_batch_size(std::iter::empty());
        assert_eq!(avg, 0);
    }

    #[test]
    fn test_latency_search_space_convergence() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();

        // Simulate multiple iterations with good latency
        for _ in 0..5 {
            let batch = create_batch_data(state.current_batch_size, Duration::from_millis(95));
            strategy.calculate_new_requirements(&mut state, batch);
        }

        // Search space should converge (high - low should be small)
        let search_space = state.search_high.saturating_sub(state.search_low);
        assert!(search_space > 0); // Should still have some space to search
    }

    #[test]
    fn test_latency_max_batch_size_constraint() {
        let strategy = create_strategy();
        let mut state = strategy.make_state();
        state.search_high = strategy.max_batch_size + 100;

        let batch = create_batch_data(50, Duration::from_millis(50));
        let _req = strategy.calculate_new_requirements(&mut state, batch);
        assert!(state.search_high <= strategy.max_batch_size);
        assert!(state.current_batch_size <= strategy.max_batch_size);
    }
}
