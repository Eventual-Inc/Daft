use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    dynamic_batching::{BatchingState, BatchingStrategy},
    pipeline::MorselSizeRequirement,
    runtime_stats::RuntimeStats,
};
pub struct DynBatchingState {
    #[allow(clippy::type_complexity)]
    record_fn: Box<dyn FnMut(Arc<dyn RuntimeStats>, usize, Duration) + Send + Sync>,
    update_fn: Box<dyn FnMut() -> MorselSizeRequirement + Send + Sync>,
}

impl BatchingState for DynBatchingState {
    fn record_execution_stat(
        &mut self,
        stats: Arc<dyn RuntimeStats>,
        batch_size: usize,
        duration: Duration,
    ) {
        (self.record_fn)(stats, batch_size, duration);
    }
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
                let state = strategy.make_state();
                let strategy_clone = strategy.clone();

                // Use Arc<Mutex<>> to share state between closures
                let shared_state = Arc::new(Mutex::new(state));
                let state_for_record = shared_state.clone();
                let state_for_update = shared_state;

                DynBatchingState {
                    record_fn: Box::new(move |stats, batch_size, duration| {
                        state_for_record
                            .lock()
                            .unwrap()
                            .record_execution_stat(stats, batch_size, duration);
                    }),
                    update_fn: Box::new(move || {
                        let mut state_guard = state_for_update.lock().unwrap();
                        strategy_clone.calculate_new_requirements(&mut *state_guard)
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

    fn calculate_new_requirements(&self, state: &mut Self::State) -> MorselSizeRequirement {
        (state.update_fn)()
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
    impl BatchingState for usize {
        fn record_execution_stat(
            &mut self,
            _stats: Arc<dyn crate::runtime_stats::RuntimeStats>,
            batch_size: usize,
            _duration: std::time::Duration,
        ) {
            *self += batch_size;
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

        fn calculate_new_requirements(&self, state: &mut Self::State) -> MorselSizeRequirement {
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
        let req1 = dyn_strategy.calculate_new_requirements(&mut state);
        assert_eq!(req1, MorselSizeRequirement::Flexible(1, 10));

        // Second call
        let req2 = dyn_strategy.calculate_new_requirements(&mut state);
        assert_eq!(req2, MorselSizeRequirement::Flexible(5, 20));

        // Third call
        let req3 = dyn_strategy.calculate_new_requirements(&mut state);
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
        let req1_1 = dyn_strategy.calculate_new_requirements(&mut state1);
        let req2_1 = dyn_strategy.calculate_new_requirements(&mut state2);

        assert_eq!(req1_1, MorselSizeRequirement::Flexible(1, 10));
        assert_eq!(req2_1, MorselSizeRequirement::Flexible(1, 10));

        // Second call on state1 should advance its internal state
        let req1_2 = dyn_strategy.calculate_new_requirements(&mut state1);
        assert_eq!(req1_2, MorselSizeRequirement::Flexible(5, 20));

        // But state2 should still be at first call
        let req2_2 = dyn_strategy.calculate_new_requirements(&mut state2);
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

        dyn1.calculate_new_requirements(&mut state1);
        dyn2.calculate_new_requirements(&mut state2);

        // Each should have been called independently
        assert_eq!(mock1.call_count(), 1);
        assert_eq!(mock2.call_count(), 1);
    }
}
