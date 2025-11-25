use crate::{
    dynamic_batching::{BatchReport, BatchingStrategy},
    pipeline::MorselSizeRequirement,
};

pub struct DynBatchingState {
    update_fn: Box<dyn FnMut(BatchReport) -> MorselSizeRequirement + Send + Sync>,
}

/// Type-erased wrapper for any `BatchingStrategy` implementation, allowing
/// dynamic batching strategies to be stored and used at runtime without
/// compile-time generic constraints.
pub struct DynBatchingStrategy {
    initial_req: MorselSizeRequirement,
    make_state_fn: Box<dyn Fn() -> DynBatchingState + Send + Sync>,
}

#[cfg(debug_assertions)]
impl std::fmt::Debug for DynBatchingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynBatchingStrategy")
            .field("initial_req", &self.initial_req)
            .finish_non_exhaustive()
    }
}

impl DynBatchingStrategy {
    pub fn new<S: BatchingStrategy + Clone + 'static>(strategy: S) -> Self
    where
        S::State: 'static,
    {
        let initial_req = strategy.initial_requirements();

        Self {
            initial_req,
            make_state_fn: Box::new(move || {
                let mut state = strategy.make_state();
                let strategy_clone = strategy.clone();

                DynBatchingState {
                    update_fn: Box::new(move |reports| {
                        strategy_clone.calculate_new_requirements(&mut state, reports)
                    }),
                }
            }),
        }
    }
}
impl<S: BatchingStrategy + Clone + 'static> From<S> for DynBatchingStrategy
where
    S::State: 'static,
{
    fn from(strategy: S) -> Self {
        Self::new(strategy)
    }
}

impl BatchingStrategy for DynBatchingStrategy {
    type State = DynBatchingState;

    fn make_state(&self) -> Self::State {
        (self.make_state_fn)()
    }

    fn calculate_new_requirements(
        &self,
        state: &mut Self::State,
        reports: BatchReport,
    ) -> MorselSizeRequirement {
        (state.update_fn)(reports)
    }

    fn initial_requirements(&self) -> MorselSizeRequirement {
        self.initial_req
    }
}
#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    // Mock strategy for testing
    #[derive(Clone, Debug)]
    struct MockStrategy {
        initial_req: MorselSizeRequirement,
        call_counter: Arc<Mutex<usize>>,
    }

    impl MockStrategy {
        fn new(initial_req: MorselSizeRequirement) -> Self {
            Self {
                initial_req,
                call_counter: Arc::new(Mutex::new(0)),
            }
        }

        fn call_count(&self) -> usize {
            *self.call_counter.lock().unwrap()
        }
    }

    impl BatchingStrategy for MockStrategy {
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
            _reports: BatchReport,
        ) -> MorselSizeRequirement {
            *self.call_counter.lock().unwrap() += 1;
            *state += 1;

            // Return different requirements based on state to test delegation
            match *state {
                1 => MorselSizeRequirement::Flexible(1, 10),
                2 => MorselSizeRequirement::Flexible(5, 20),
                _ => MorselSizeRequirement::Flexible(10, 50),
            }
        }
    }

    #[test]
    fn test_dyn_strategy_new() {
        let mock = MockStrategy::new(MorselSizeRequirement::Flexible(1, 32));
        let dyn_strategy = DynBatchingStrategy::new(mock);

        assert_eq!(
            dyn_strategy.initial_requirements(),
            MorselSizeRequirement::Flexible(1, 32)
        );
    }

    #[test]
    fn test_dyn_strategy_from() {
        let mock = MockStrategy::new(MorselSizeRequirement::Flexible(2, 64));
        let dyn_strategy: DynBatchingStrategy = mock.into();

        assert_eq!(
            dyn_strategy.initial_requirements(),
            MorselSizeRequirement::Flexible(2, 64)
        );
    }

    #[test]
    fn test_dyn_strategy_delegates_to_wrapped_strategy() {
        let mock = MockStrategy::new(MorselSizeRequirement::Flexible(1, 16));
        let dyn_strategy = DynBatchingStrategy::new(mock.clone());
        let mut state = dyn_strategy.make_state();

        // First call
        let req1 = dyn_strategy.calculate_new_requirements(&mut state, vec![]);
        assert_eq!(req1, MorselSizeRequirement::Flexible(1, 10));

        // Second call
        let req2 = dyn_strategy.calculate_new_requirements(&mut state, vec![]);
        assert_eq!(req2, MorselSizeRequirement::Flexible(5, 20));

        // Third call
        let req3 = dyn_strategy.calculate_new_requirements(&mut state, vec![]);
        assert_eq!(req3, MorselSizeRequirement::Flexible(10, 50));

        // Verify the wrapped strategy was called
        assert_eq!(mock.call_count(), 3);
    }

    #[test]
    fn test_dyn_strategy_state_isolation() {
        let mock = MockStrategy::new(MorselSizeRequirement::Flexible(1, 8));
        let dyn_strategy = DynBatchingStrategy::new(mock);

        let mut state1 = dyn_strategy.make_state();
        let mut state2 = dyn_strategy.make_state();

        // Each state should be independent
        let req1_1 = dyn_strategy.calculate_new_requirements(&mut state1, vec![]);
        let req2_1 = dyn_strategy.calculate_new_requirements(&mut state2, vec![]);

        assert_eq!(req1_1, MorselSizeRequirement::Flexible(1, 10));
        assert_eq!(req2_1, MorselSizeRequirement::Flexible(1, 10));

        // Second call on state1 should advance its internal state
        let req1_2 = dyn_strategy.calculate_new_requirements(&mut state1, vec![]);
        assert_eq!(req1_2, MorselSizeRequirement::Flexible(5, 20));

        // But state2 should still be at first call
        let req2_2 = dyn_strategy.calculate_new_requirements(&mut state2, vec![]);
        assert_eq!(req2_2, MorselSizeRequirement::Flexible(5, 20));
    }

    #[test]
    fn test_dyn_strategy_multiple_instances() {
        let mock1 = MockStrategy::new(MorselSizeRequirement::Flexible(1, 16));
        let mock2 = MockStrategy::new(MorselSizeRequirement::Flexible(4, 32));

        let dyn1 = DynBatchingStrategy::new(mock1.clone());
        let dyn2 = DynBatchingStrategy::new(mock2.clone());

        assert_eq!(
            dyn1.initial_requirements(),
            MorselSizeRequirement::Flexible(1, 16)
        );
        assert_eq!(
            dyn2.initial_requirements(),
            MorselSizeRequirement::Flexible(4, 32)
        );

        let mut state1 = dyn1.make_state();
        let mut state2 = dyn2.make_state();

        dyn1.calculate_new_requirements(&mut state1, vec![]);
        dyn2.calculate_new_requirements(&mut state2, vec![]);

        // Each should have been called independently
        assert_eq!(mock1.call_count(), 1);
        assert_eq!(mock2.call_count(), 1);
    }
}
