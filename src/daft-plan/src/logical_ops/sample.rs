use std::sync::Arc;

use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Sample {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub fraction: String,
    pub with_replacement: bool,
    pub seed: Option<u64>,
}

impl Sample {
    pub(crate) fn new(
        input: Arc<LogicalPlan>,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> Self {
        Self {
            input,
            fraction: fraction.to_string(),
            with_replacement,
            seed,
        }
    }
}
