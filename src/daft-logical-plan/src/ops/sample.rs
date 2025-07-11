use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    stats::{PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub fraction: f64,
    pub with_replacement: bool,
    pub seed: Option<u64>,
    pub stats_state: StatsState,
}

impl Eq for Sample {}

impl Hash for Sample {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the `input` field.
        self.input.hash(state);

        // Convert the `f64` to a stable format with 6 decimal places.
        #[expect(clippy::collection_is_never_read, reason = "nursery bug pretty sure")]
        let fraction_str = format!("{:.6}", self.fraction);
        fraction_str.hash(state);

        // Hash the rest of the fields.
        self.with_replacement.hash(state);
        self.seed.hash(state);
    }
}

impl Sample {
    pub(crate) fn new(
        input: Arc<LogicalPlan>,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            fraction,
            with_replacement,
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
        // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
        let input_stats = self.input.materialized_stats();
        let approx_stats = input_stats
            .approx_stats
            .apply(|v| ((v as f64) * self.fraction) as usize);
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Sample: {}", self.fraction));
        res.push(format!("With replacement = {}", self.with_replacement));
        res.push(format!("Seed = {:?}", self.seed));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
