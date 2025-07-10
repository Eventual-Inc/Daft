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
    // Offset on number of rows. Maybe a new Struct should be introduced to simplify the logic here,
    // such as Slice
    pub offset: Option<u64>,
    // Limit on number of rows.
    pub limit: Option<u64>,
    // Whether to send tasks in waves (maximize throughput) or
    // eagerly one-at-a-time (maximize time-to-first-result)
    pub eager: bool,
    pub stats_state: StatsState,
}

impl Limit {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        offset: Option<u64>,
        limit: Option<u64>,
        eager: bool,
    ) -> Result<Self> {
        if offset.is_none() && limit.is_none() {
            return Err(DaftError::InternalError(
                "LIMIT node must have offset or limit".to_string(),
            ))
            .context(CreationSnafu);
        }

        if let (Some(o), Some(l)) = (offset, limit) {
            if o >= l {
                return Err(DaftError::InternalError(format!(
                    "LIMIT node's offset {} must be less than limit {}",
                    o, l
                )))
                .context(CreationSnafu);
            }
        }

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            offset,
            limit,
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

        let limit_num_rows = match (self.offset, self.limit) {
            (Some(o), Some(l)) => (l - o) as usize,
            (Some(o), None) => approx_num_rows.saturating_sub(o as usize),
            (None, Some(l)) => l as usize,
            (None, None) => unreachable!(),
        };

        let limit_selectivity = if approx_num_rows > limit_num_rows {
            if approx_num_rows == 0 {
                0.0
            } else {
                limit_num_rows as f64 / approx_num_rows as f64
            }
        } else {
            1.0
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
        let mut res = vec![match (self.offset, self.limit) {
            (Some(o), Some(l)) => format!("Limit: {}, Offset = {}", l, o),
            (Some(o), None) => format!("Limit: N/A, Offset = {}", o),
            (None, Some(l)) => format!("Limit: {}", l),
            (None, None) => unreachable!(),
        }];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
