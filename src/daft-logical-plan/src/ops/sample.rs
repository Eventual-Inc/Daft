use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    LogicalPlan,
    stats::{PlanStats, StatsState},
};

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Sample {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub fraction: Option<f64>,
    pub size: Option<usize>,
    pub with_replacement: bool,
    pub seed: Option<u64>,
    pub stats_state: StatsState,
}

impl Eq for Sample {}

impl Hash for Sample {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the `input` field.
        self.input.hash(state);

        // Hash fraction if present (rounded to 6 decimals to avoid tiny differences)
        if let Some(fraction) = self.fraction {
            format!("{:.6}", fraction).hash(state);
        }

        // Hash size if present
        if let Some(size) = self.size {
            size.hash(state);
        }

        // Hash the rest of the fields.
        self.with_replacement.hash(state);
        self.seed.hash(state);
    }
}

impl Sample {
    pub(crate) fn new(
        input: Arc<LogicalPlan>,
        fraction: Option<f64>,
        size: Option<usize>,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            fraction,
            size,
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
        let approx_stats = if let Some(fraction) = self.fraction {
            input_stats
                .approx_stats
                .apply(|v| ((v as f64) * fraction) as usize)
        } else if let Some(size) = self.size {
            input_stats.approx_stats.apply(|v| v.min(size))
        } else {
            input_stats.approx_stats.clone()
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(fraction) = self.fraction {
            res.push(format!("Sample: {} (fraction)", fraction));
        } else if let Some(size) = self.size {
            res.push(format!("Sample: {} rows", size));
        }
        res.push(format!("With replacement = {}", self.with_replacement));
        res.push(format!("Seed = {:?}", self.seed));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
