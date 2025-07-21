use std::sync::Arc;

use daft_stats::plan_stats::{calculate::calculate_limit_stats, StatsState};
use serde::{Deserialize, Serialize};

use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Limit {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Limit on number of rows.
    pub limit: u64,
    // Whether to send tasks in waves (maximize throughput) or
    // eagerly one-at-a-time (maximize time-to-first-result)
    pub eager: bool,
    pub stats_state: StatsState,
}

impl Limit {
    pub(crate) fn new(input: Arc<LogicalPlan>, limit: u64, eager: bool) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            limit,
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
        let stats = calculate_limit_stats(input_stats, self.limit);
        self.stats_state = StatsState::Materialized(stats.into());
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
