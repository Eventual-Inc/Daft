use std::sync::Arc;

use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Limit {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Limit on number of rows.
    pub limit: i64,
    // Whether to send tasks in waves (maximize throughput) or
    // eagerly one-at-a-time (maximize time-to-first-result)
    pub eager: bool,
}

impl Limit {
    pub(crate) fn new(input: Arc<LogicalPlan>, limit: i64, eager: bool) -> Self {
        Self {
            input,
            limit,
            eager,
        }
    }
}
