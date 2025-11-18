use std::sync::Arc;

use daft_dsl::functions::prelude::{Deserialize, Serialize};

use crate::{
    LogicalPlan,
    stats::{ApproxStats, PlanStats, StatsState},
};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Offset {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Offset on number of rows.
    pub offset: u64,
    pub stats_state: StatsState,
}

impl Offset {
    pub(crate) fn new(input: Arc<LogicalPlan>, offset: u64) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            offset,
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
        let approx_num_rows = input_stats.approx_stats.num_rows;

        let offset_num_rows = approx_num_rows.saturating_sub(self.offset as usize);
        let offset_selectivity = if approx_num_rows == 0 {
            0.0
        } else {
            (offset_num_rows as f64 / approx_num_rows as f64).clamp(0.0, 1.0)
        };

        let approx_stats = ApproxStats {
            num_rows: offset_num_rows,
            size_bytes: offset_num_rows
                * (input_stats.approx_stats.size_bytes / approx_num_rows.max(1)),
            acc_selectivity: input_stats.approx_stats.acc_selectivity * offset_selectivity,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("Offset: {}", self.offset)];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
