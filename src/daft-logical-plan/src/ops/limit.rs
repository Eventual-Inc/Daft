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
        let limit_selectivity = if input_stats.approx_stats.num_rows > limit {
            if input_stats.approx_stats.num_rows == 0 {
                0.0
            } else {
                limit as f64 / input_stats.approx_stats.num_rows as f64
            }
        } else {
            1.0
        };
        let approx_stats = ApproxStats {
            num_rows: limit.min(input_stats.approx_stats.num_rows),
            size_bytes: if input_stats.approx_stats.num_rows > limit {
                let est_bytes_per_row =
                    input_stats.approx_stats.size_bytes / input_stats.approx_stats.num_rows.max(1);
                limit * est_bytes_per_row
            } else {
                input_stats.approx_stats.size_bytes
            },
            acc_selectivity: input_stats.approx_stats.acc_selectivity * limit_selectivity,
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
