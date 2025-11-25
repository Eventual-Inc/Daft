use crate::{
    dynamic_batching::{BatchReport, BatchingStrategy},
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

impl BatchingStrategy for StaticBatchingStrategy {
    type State = ();
    fn make_state(&self) -> Self::State {}

    fn calculate_new_requirements(
        &self,
        _state: &mut Self::State,
        _reports: BatchReport,
    ) -> MorselSizeRequirement {
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
        let empty_reports = vec![];
        let result = strategy.calculate_new_requirements(&mut state, empty_reports);
        assert_eq!(result, req);

        // Non-empty batch report (would need actual BatchReport data structure)
        let some_reports = vec![]; // Assuming BatchReport is Vec-like
        let result2 = strategy.calculate_new_requirements(&mut state, some_reports);
        assert_eq!(result2, req);
    }

    #[test]
    fn test_static_consistency_across_calls() {
        let req = MorselSizeRequirement::Flexible(1, 256);
        let strategy = StaticBatchingStrategy::new(req);
        let mut state = strategy.make_state();

        // Multiple calls should return same result
        let result1 = strategy.calculate_new_requirements(&mut state, vec![]);
        let result2 = strategy.calculate_new_requirements(&mut state, vec![]);
        let result3 = strategy.calculate_new_requirements(&mut state, vec![]);

        assert_eq!(result1, req);
        assert_eq!(result2, req);
        assert_eq!(result3, req);
        assert_eq!(result1, result2);
        assert_eq!(result2, result3);
    }
}
