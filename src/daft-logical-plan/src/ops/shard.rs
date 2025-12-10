use std::sync::Arc;

use common_scan_info::Sharder;
use serde::{Deserialize, Serialize};

use crate::{
    LogicalPlan,
    stats::{ApproxStats, PlanStats, StatsState},
};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Shard {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Sharder.
    pub sharder: Sharder,
    pub stats_state: StatsState,
}

impl Shard {
    pub(crate) fn new(input: Arc<LogicalPlan>, sharder: Sharder) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            sharder,
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
        let approx_stats = ApproxStats {
            num_rows: input_stats.approx_stats.num_rows / self.sharder.world_size(),
            size_bytes: input_stats.approx_stats.size_bytes / self.sharder.world_size(),
            acc_selectivity: input_stats.approx_stats.acc_selectivity
                / self.sharder.world_size() as f64,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!(
            "Shard: strategy={}, world_size={}, rank={}",
            self.sharder.strategy(),
            self.sharder.world_size(),
            self.sharder.rank()
        )];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
