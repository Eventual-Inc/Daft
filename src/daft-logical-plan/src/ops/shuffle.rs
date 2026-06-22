use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{LogicalPlan, stats::StatsState};

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Shuffle {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub input: Arc<LogicalPlan>,
    pub seed: Option<u64>,
    pub stats_state: StatsState,
}

impl Eq for Shuffle {}

impl Hash for Shuffle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.seed.hash(state);
    }
}

impl Shuffle {
    pub(crate) fn new(input: Arc<LogicalPlan>, seed: Option<u64>) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            seed,
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
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![String::from(
            "Shuffle: random row order (via random_int + sort)",
        )];
        res.push(format!("Seed = {:?}", self.seed));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
