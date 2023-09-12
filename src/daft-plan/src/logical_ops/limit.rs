use std::sync::Arc;

use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Limit {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Limit on number of rows.
    pub limit: i64,
}

impl Limit {
    pub(crate) fn new(input: Arc<LogicalPlan>, limit: i64) -> Self {
        Self { input, limit }
    }
}
