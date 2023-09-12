use std::sync::Arc;

use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Coalesce {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Number of partitions to coalesce to.
    pub num_to: usize,
}

impl Coalesce {
    pub(crate) fn new(input: Arc<LogicalPlan>, num_to: usize) -> Self {
        Self { input, num_to }
    }
}
