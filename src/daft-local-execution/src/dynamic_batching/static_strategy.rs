use crate::{
    dynamic_batching::{BatchingState, BatchingStrategy},
    pipeline::MorselSizeRequirement,
};

/// A batching strategy that maintains fixed batch requirements regardless
/// of execution performance, useful when dynamic adjustment is not desired.
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct StaticBatchingStrategy {
    req: MorselSizeRequirement,
}

impl StaticBatchingStrategy {
    pub fn new(req: MorselSizeRequirement) -> Self {
        Self { req }
    }
}
impl BatchingState for () {
    fn record_execution_stat(
        &mut self,
        _stats: std::sync::Arc<dyn crate::runtime_stats::RuntimeStats>,
        _batch_size: usize,
        _duration: std::time::Duration,
    ) {
    }
}

impl BatchingStrategy for StaticBatchingStrategy {
    type State = ();
    fn make_state(&self) -> Self::State {}

    fn calculate_new_requirements(&self, _state: &mut Self::State) -> MorselSizeRequirement {
        self.req
    }

    fn initial_requirements(&self) -> MorselSizeRequirement {
        self.req
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_strategy_creation() {
        let req = MorselSizeRequirement::Flexible(1, 32);
        let strategy = StaticBatchingStrategy::new(req);

        assert_eq!(strategy.req, req);
    }

    #[test]
    fn test_static_initial_requirements() {
        let req = MorselSizeRequirement::Flexible(5, 100);
        let strategy = StaticBatchingStrategy::new(req);

        assert_eq!(strategy.initial_requirements(), req);
    }

    #[test]
    fn test_static_calculate_requirements_ignores_reports() {
        let req = MorselSizeRequirement::Flexible(2, 64);
        let strategy = StaticBatchingStrategy::new(req);
        let mut state = strategy.make_state();

        // Empty batch report
        let result = strategy.calculate_new_requirements(&mut state);
        assert_eq!(result, req);

        // Non-empty batch report (would need actual BatchReport data structure)
        let result2 = strategy.calculate_new_requirements(&mut state);
        assert_eq!(result2, req);
    }

    #[test]
    fn test_static_consistency_across_calls() {
        let req = MorselSizeRequirement::Flexible(1, 256);
        let strategy = StaticBatchingStrategy::new(req);
        let mut state = strategy.make_state();

        // Multiple calls should return same result
        let result1 = strategy.calculate_new_requirements(&mut state);
        let result2 = strategy.calculate_new_requirements(&mut state);
        let result3 = strategy.calculate_new_requirements(&mut state);

        assert_eq!(result1, req);
        assert_eq!(result2, req);
        assert_eq!(result3, req);
        assert_eq!(result1, result2);
        assert_eq!(result2, result3);
    }
}
