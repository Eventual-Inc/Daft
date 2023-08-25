use std::sync::Arc;

use crate::LogicalPlan;

#[derive(Clone, Debug)]
pub struct Concat {
    // Upstream nodes.
    pub input: Arc<LogicalPlan>,
    pub other: Arc<LogicalPlan>,
}

impl Concat {
    pub(crate) fn new(input: Arc<LogicalPlan>, other: Arc<LogicalPlan>) -> Self {
        Self { input, other }
    }
}
