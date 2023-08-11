use std::sync::Arc;

use crate::LogicalPlan;

#[derive(Clone, Debug)]
pub struct Coalesce {
    // Number of partitions to coalesce to.
    pub num_to: usize,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Coalesce {
    pub(crate) fn new(num_to: usize, input: Arc<LogicalPlan>) -> Self {
        Self { num_to, input }
    }
}
