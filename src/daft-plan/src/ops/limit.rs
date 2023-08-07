use std::sync::Arc;

use crate::LogicalPlan;

#[derive(Clone, Debug)]
pub struct Limit {
    pub limit: i64,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Limit {
    pub(crate) fn new(limit: i64, input: Arc<LogicalPlan>) -> Self {
        Self { limit, input }
    }
}
