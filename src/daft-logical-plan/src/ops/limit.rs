use std::sync::Arc;

use crate::{
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Limit {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Limit on number of rows.
    pub limit: i64,
    // Whether to send tasks in waves (maximize throughput) or
    // eagerly one-at-a-time (maximize time-to-first-result)
    pub eager: bool,
    pub stats_state: StatsState,
}

impl Limit {
    pub(crate) fn new(input: Arc<LogicalPlan>, limit: i64, eager: bool) -> Self {
        Self {
            input,
            limit,
            eager,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let input_stats = self.input.materialized_stats();
        let limit = self.limit as usize;
        let est_bytes_per_row_lower = input_stats.approx_stats.lower_bound_bytes
            / input_stats.approx_stats.lower_bound_rows.max(1);
        let est_bytes_per_row_upper =
            input_stats
                .approx_stats
                .upper_bound_bytes
                .and_then(|bytes| {
                    input_stats
                        .approx_stats
                        .upper_bound_rows
                        .map(|rows| bytes / rows.max(1))
                });
        let new_lower_rows = input_stats.approx_stats.lower_bound_rows.min(limit);
        let new_upper_rows = input_stats
            .approx_stats
            .upper_bound_rows
            .map(|ub| ub.min(limit))
            .unwrap_or(limit);
        let approx_stats = ApproxStats {
            lower_bound_rows: new_lower_rows,
            upper_bound_rows: Some(new_upper_rows),
            lower_bound_bytes: new_lower_rows * est_bytes_per_row_lower,
            upper_bound_bytes: est_bytes_per_row_upper.map(|x| x * new_upper_rows),
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("Limit: {}", self.limit)];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
