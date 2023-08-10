use std::sync::Arc;

use crate::LogicalPlan;

#[derive(Clone, Debug)]
pub struct Concat {
    pub other: Arc<LogicalPlan>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Concat {
    pub(crate) fn new(other: Arc<LogicalPlan>, input: Arc<LogicalPlan>) -> Self {
        Self { other, input }
    }
}
