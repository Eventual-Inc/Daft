use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    LogicalPlan,
    stats::{ApproxStats, PlanStats, StatsState},
};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Limit {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Limit on number of rows.
    pub limit: u64,
    // Offset on number of rows. This is optional, it's equivalent to offset = 0 if not passed.
    pub offset: Option<u64>,
    // Whether to send tasks in waves (maximize throughput) or
    // eagerly one-at-a-time (maximize time-to-first-result)
    pub eager: bool,
    pub stats_state: StatsState,
}

impl Limit {
    pub(crate) fn new(
        input: Arc<LogicalPlan>,
        limit: u64,
        offset: Option<u64>,
        eager: bool,
    ) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            limit,
            offset,
            eager,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let input_stats = self.input.materialized_stats();
        let limit = (self.limit + self.offset.unwrap_or(0)) as usize;
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
        let mut res = vec![match &self.offset {
            Some(offset) => format!("Limit: Num Rows = {}, Offset = {}", self.limit, offset),
            None => format!("Limit: {}", self.limit),
        }];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
