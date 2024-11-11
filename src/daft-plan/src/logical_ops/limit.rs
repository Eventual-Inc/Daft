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

use crate::stats::{ApproxStats, Stats};
impl Stats for Limit {
    fn approximate_stats(&self) -> ApproxStats {
        let limit = self.limit as usize;
        let input_stats = self.input.approximate_stats();
        let est_bytes_per_row_lower =
            input_stats.lower_bound_bytes / (input_stats.lower_bound_rows.max(1));
        let est_bytes_per_row_upper = input_stats
            .upper_bound_bytes
            .and_then(|bytes| input_stats.upper_bound_rows.map(|rows| bytes / rows.max(1)));
        let new_lower_rows = input_stats.lower_bound_rows.min(limit);
        let new_upper_rows = input_stats
            .upper_bound_rows
            .map(|ub| ub.min(limit))
            .unwrap_or(limit);
        ApproxStats {
            lower_bound_rows: new_lower_rows,
            upper_bound_rows: Some(new_upper_rows),
            lower_bound_bytes: new_lower_rows * est_bytes_per_row_lower,
            upper_bound_bytes: est_bytes_per_row_upper.map(|x| x * new_upper_rows),
        }
    }
}
