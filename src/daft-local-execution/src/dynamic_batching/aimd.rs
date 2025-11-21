use std::time::Duration;

use crate::{dynamic_batching::BatchingStrategy, runtime_stats::RuntimeStats};

/// Additive Increase/Multiplicative Decrease (AIMD) algorithm for dynamic batch sizing.
///
/// AIMD is a feedback control algorithm originally developed for TCP congestion control
/// that combines linear growth when performance is acceptable with exponential reduction
/// when congestion is detected. This implementation adapts the algorithm for batch size
/// optimization based on execution latency.
///
/// # Algorithm Behavior
///
/// - **Additive Increase**: When latency is below the threshold, increase batch size by
///   a fixed amount
/// - **Multiplicative Decrease**: When latency exceeds the threshold, multiply batch size
///   by a factor less than 1.0 (exponential reduction)
/// - **Result**: Creates a "sawtooth" pattern that oscillates around the optimal batch size
///
/// # Advantages
///
/// - Simple and well-understood algorithm with decades of research
/// - Proven convergence properties when multiple flows compete for resources
/// - Self-adapting to changing system conditions
/// - Conservative approach that avoids sustained overload
///
/// # Disadvantages
///
/// - Inherently oscillatory behavior can cause inconsistent performance
/// - Slow ramp-up from small batch sizes due to additive increase
/// - Binary threshold decision may be too harsh for some workloads
///
/// # Example
///
/// ```rust,ignore
///
/// let mut aimd = AimdBatching::new(
///     10,                            // 10 additive increase
///     0.5,                           // 50% multiplicative decrease
///     Duration::from_millis(100),    // 100ms latency threshold
///     1,                             // minimum batch size
///     10000,                         // maximum batch size
/// );
///
///;
///
/// // Simulate good performance - batch size will grow by 10%
/// aimd.adjust_batch_size(Duration::from_millis(50));
///
/// // Simulate congestion - batch size will be cut in half
/// aimd.adjust_batch_size(Duration::from_millis(150));
/// ```
pub struct AimdBatching {
    additive_increase: usize,
    multiplicative_decrease: f64,
    latency_threshold: Duration,
    min_batch_size: usize,
    max_batch_size: usize,
    state: AimdState,
}

#[allow(dead_code)]
impl AimdBatching {
    pub fn new(
        additive_increase: usize,
        multiplicative_decrease: f64,
        latency_threshold: Duration,
        min_batch_size: usize,
        max_batch_size: usize,
    ) -> Self {
        Self {
            additive_increase,
            multiplicative_decrease,
            latency_threshold,
            min_batch_size,
            max_batch_size,
            state: AimdState::new(min_batch_size),
        }
    }
    pub fn with_increase_amount(mut self, v: usize) -> Self {
        self.additive_increase = v;
        self
    }
}
impl Default for AimdBatching {
    fn default() -> Self {
        Self {
            additive_increase: 16,
            multiplicative_decrease: 0.5, // 50% decrease
            latency_threshold: Duration::from_secs_f64(0.5),
            min_batch_size: 1,
            max_batch_size: 128 * 1024,
            state: AimdState::new(1),
        }
    }
}

pub struct AimdState {
    current_batch_size: usize,
}

impl AimdState {
    pub fn new(initial_batch_size: usize) -> Self {
        Self {
            current_batch_size: initial_batch_size,
        }
    }
}

impl BatchingStrategy for AimdBatching {
    fn adjust_batch_size(
        &mut self,
        _runtime_stats: &dyn RuntimeStats,
        duration: Duration,
    ) -> usize {
        let state = &mut self.state;

        let congestion_detected = duration > self.latency_threshold;

        state.current_batch_size = if congestion_detected {
            // Multiplicative decrease
            ((state.current_batch_size as f64 * self.multiplicative_decrease) as usize)
                .max(self.min_batch_size)
        } else {
            (state.current_batch_size + self.additive_increase).min(self.max_batch_size)
        };

        state.current_batch_size
    }

    fn current_batch_size(&self) -> usize {
        self.state.current_batch_size
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{dynamic_batching::BatchingStrategy, runtime_stats::DefaultRuntimeStats};

    #[test]
    fn test_aimd_additive_increase() {
        let mut aimd = AimdBatching::default().with_increase_amount(20);
        let rt_stats = DefaultRuntimeStats::new(0);
        let new_size = aimd.adjust_batch_size(&rt_stats, Duration::from_millis(100));
        assert_eq!(new_size, 21);

        // Set a reasonable starting size
        aimd.state.current_batch_size = 100;
        let new_size = aimd.adjust_batch_size(&rt_stats, Duration::from_millis(100));
        assert_eq!(new_size, 120);
    }

    #[test]
    fn test_aimd_multiplicative_decrease() {
        let mut aimd = AimdBatching::default();
        let rt_stats = DefaultRuntimeStats::new(0);

        aimd.state.current_batch_size = 1000;

        // Slow execution should cut batch size in half
        let new_size = aimd.adjust_batch_size(&rt_stats, Duration::from_secs(1));
        assert_eq!(new_size, 500); // 1000 * 0.5
    }

    #[test]
    fn test_aimd_bounds() {
        let mut aimd = AimdBatching::new(50, 0.1, Duration::from_millis(100), 10, 200);
        let rt_stats = DefaultRuntimeStats::new(0);

        // Should respect min bound
        aimd.state.current_batch_size = 15;
        let new_size = aimd.adjust_batch_size(&rt_stats, Duration::from_secs(1));
        assert_eq!(new_size, 10); // max(15 + 50, 10) = 10

        // Should respect max bound
        aimd.state.current_batch_size = 190;
        let new_size = aimd.adjust_batch_size(&rt_stats, Duration::from_millis(50));
        assert_eq!(new_size, 200); // min(190 + 50, 200) = 200
    }

    #[test]
    fn test_aimd_sawtooth_pattern() {
        let mut aimd = AimdBatching::default().with_increase_amount(10);
        let rt_stats = DefaultRuntimeStats::new(0);
        aimd.state.current_batch_size = 100;

        // Simulate the sawtooth: increase, increase, decrease
        let size1 = aimd.adjust_batch_size(&rt_stats, Duration::from_millis(100)); // 110
        let size2 = aimd.adjust_batch_size(&rt_stats, Duration::from_millis(100)); // 120
        let size3 = aimd.adjust_batch_size(&rt_stats, Duration::from_secs(1)); // 60

        assert!(size2 > size1); // Should increase
        assert!(size3 < size2); // Should decrease
    }
    #[test]
    fn test_aimd_threshold_boundary_conditions() {
        let mut aimd = AimdBatching::new(10, 0.5, Duration::from_millis(100), 1, 1000);
        let rt_stats = DefaultRuntimeStats::new(0);
        aimd.state.current_batch_size = 100;

        // Exactly at threshold - should still be "good" (< threshold)
        let new_size = aimd.adjust_batch_size(&rt_stats, Duration::from_millis(100));
        assert_eq!(new_size, 110); // Should increase

        // Just over threshold
        let new_size = aimd.adjust_batch_size(&rt_stats, Duration::from_millis(101));
        assert_eq!(new_size, 55); // Should decrease
    }

    #[test]
    fn test_aimd_repeated_congestion() {
        let mut aimd = AimdBatching::default();
        let rt_stats = DefaultRuntimeStats::new(0);
        aimd.state.current_batch_size = 1000;

        // Multiple decreases should keep cutting in half
        let size1 = aimd.adjust_batch_size(&rt_stats, Duration::from_secs(1)); // 500
        let size2 = aimd.adjust_batch_size(&rt_stats, Duration::from_secs(1)); // 250
        let size3 = aimd.adjust_batch_size(&rt_stats, Duration::from_secs(1)); // 125

        assert_eq!(size1, 500);
        assert_eq!(size2, 250);
        assert_eq!(size3, 125);
    }
    #[test]
    fn test_aimd_current_batch_size_getter() {
        let mut aimd = AimdBatching::default();
        aimd.state.current_batch_size = 42;

        assert_eq!(aimd.current_batch_size(), 42);
    }
}
