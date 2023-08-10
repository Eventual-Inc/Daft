use std::sync::Arc;

use crate::LogicalPlan;

#[derive(Clone, Debug)]
pub struct Distinct {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Distinct {
    pub(crate) fn new(input: Arc<LogicalPlan>) -> Self {
        Self { input }
    }
}
