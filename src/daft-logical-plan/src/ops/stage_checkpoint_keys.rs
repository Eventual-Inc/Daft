use std::sync::Arc;

use daft_common::checkpoint_config::CheckpointConfig;
use educe::Educe;
use serde::{Deserialize, Serialize};

use crate::{
    LogicalPlan,
    stats::{PlanStats, StatsState},
};

/// Side-effecting pass-through node emitted by `RewriteCheckpointSource`.
///
/// Sits immediately above the checkpoint anti-join. Rows flowing through this
/// node have their key-column values staged into the checkpoint store so that
/// re-runs of the query skip them — regardless of what downstream map-only
/// operators (filter, project, UDF) do with the row afterwards.
///
/// The node is a pushdown barrier: filter / project / limit pushdown rules
/// must not move user operators below it, otherwise post-filter keys would
/// be staged instead of post-anti-join keys, defeating source-oriented
/// tracking.
#[derive(Educe, Clone, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[educe(PartialEq, Eq, Hash)]
pub struct StageCheckpointKeys {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub input: Arc<LogicalPlan>,
    pub checkpoint_config: CheckpointConfig,
    pub stats_state: StatsState,
}

impl StageCheckpointKeys {
    pub(crate) fn new(input: Arc<LogicalPlan>, checkpoint_config: CheckpointConfig) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            checkpoint_config,
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
        self.stats_state =
            StatsState::Materialized(PlanStats::new(input_stats.approx_stats.clone()).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!(
            "StageCheckpointKeys: key_column={}",
            self.checkpoint_config.key_column
        )];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
