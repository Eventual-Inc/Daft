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
