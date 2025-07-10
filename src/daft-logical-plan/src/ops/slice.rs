use std::sync::Arc;

use common_error::{ensure, DaftResult};
use daft_dsl::functions::prelude::{Deserialize, Serialize};

use crate::{
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Slice {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Offset on number of rows.
    pub offset: Option<u64>,
    // Limit on number of rows.
    pub limit: Option<u64>,
    // Whether to send tasks in waves (maximize throughput) or
    // eagerly one-at-a-time (maximize time-to-first-result)
    pub eager: bool,
    pub stats_state: StatsState,
}

impl Slice {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        offset: Option<u64>,
        limit: Option<u64>,
        eager: bool,
    ) -> DaftResult<Self> {
        // TODO add param check and try_new for other slices, by zhenchao 2025-07-11 12:17:24
        ensure!(offset.is_some() || limit.is_some(), ValueError:"Invalid Slice, offset and limit are both None.");

        if let (Some(o), Some(l)) = (offset, limit) {
            ensure!(o < l, ValueError:"Invalid Slice, offset[{}] must be less than limit[{}]", o, l);
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

    // FIXME check correct? by zhenchao 2025-07-10 19:41:25
    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let input_stats = self.input.materialized_stats();

        let approx_row_nums = input_stats.approx_stats.num_rows as u64;
        let (offset, limit) = (self.offset, self.limit);

        let num_rows = match (offset, limit) {
            (Some(o), Some(l)) => approx_row_nums.min(l - o),
            (Some(o), None) => approx_row_nums.saturating_sub(o),
            (None, Some(l)) => approx_row_nums.min(l),
            (None, None) => unreachable!("Invalid Slice, offset and limit are both None."),
        } as usize;

        let slice_selectivity = if approx_row_nums > 0 {
            (num_rows as f64 / approx_row_nums as f64).clamp(0.0, 1.0)
        } else {
            1.0
        };

        let est_bytes_per_row = input_stats
            .approx_stats
            .size_bytes
            .saturating_div(input_stats.approx_stats.num_rows.max(1));

        let approx_stats = ApproxStats {
            num_rows: num_rows.min(input_stats.approx_stats.num_rows),
            size_bytes: est_bytes_per_row.saturating_mul(num_rows),
            acc_selectivity: input_stats.approx_stats.acc_selectivity * slice_selectivity,
        };

        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![match (self.offset, self.limit) {
            (Some(o), Some(l)) => format!("Slice: {}..{}", o, l),
            (Some(o), None) => format!("Slice: {}..", o),
            (None, Some(l)) => format!("Slice: ..{}", l),
            (None, None) => unreachable!("Invalid Slice, offset and limit are both None."),
        }];

        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
