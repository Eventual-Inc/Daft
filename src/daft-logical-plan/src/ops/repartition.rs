use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{LogicalPlan, partitioning::RepartitionSpec, stats::StatsState};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Repartition {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub repartition_spec: RepartitionSpec,
    pub stats_state: StatsState,
}

impl Repartition {
    pub(crate) fn new(input: Arc<LogicalPlan>, repartition_spec: RepartitionSpec) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            repartition_spec,
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
        // Repartitioning does not affect cardinality.
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Repartition: Scheme = {}",
            self.repartition_spec.var_name(),
        ));
        res.extend(self.repartition_spec.multiline_display());
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
