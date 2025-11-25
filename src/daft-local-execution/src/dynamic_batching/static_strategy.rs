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
