use std::sync::Arc;

use common_error::DaftError;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::{
    logical_plan::{CreationSnafu, Result},
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        limit: u64,
        offset: Option<u64>,
        eager: bool,
    ) -> Result<Self> {
        if offset.is_some_and(|o| o > limit) {
            return Err(DaftError::ValueError(format!(
                "LIMIT node's offset {} is greater than limit {}",
                offset.unwrap(),
                limit
            )))
            .context(CreationSnafu);
        }

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            limit,
            offset,
            eager,
            stats_state: StatsState::NotMaterialized,
        })
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
        let approx_num_rows = input_stats.approx_stats.num_rows;

        let limit_num_rows = match self.offset {
            Some(o) => (self.limit - o) as usize,
            None => self.limit as usize,
        };

        let limit_selectivity = if approx_num_rows == 0 {
            0.0
        } else {
            (limit_num_rows as f64 / approx_num_rows as f64).clamp(0.0, 1.0)
        };

        let approx_stats = ApproxStats {
            num_rows: limit_num_rows.min(approx_num_rows),
            size_bytes: if approx_num_rows > limit_num_rows {
                let est_bytes_per_row =
                    input_stats.approx_stats.size_bytes / approx_num_rows.max(1);
                limit_num_rows * est_bytes_per_row
            } else {
                input_stats.approx_stats.size_bytes
            },
            acc_selectivity: input_stats.approx_stats.acc_selectivity * limit_selectivity,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![match self.offset {
            Some(o) => format!("Limit: Num Rows = {}, Offset = {}", self.limit - o, o),
            None => format!("Limit: {}", self.limit),
        }];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
