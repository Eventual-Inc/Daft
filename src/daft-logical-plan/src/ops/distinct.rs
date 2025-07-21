use std::sync::Arc;

use daft_dsl::ExprRef;
use daft_stats::plan_stats::{calculate::calculate_distinct_stats, StatsState};
use serde::{Deserialize, Serialize};

use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Distinct {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub stats_state: StatsState,
    pub columns: Option<Vec<ExprRef>>,
}

impl Distinct {
    pub(crate) fn new(input: Arc<LogicalPlan>, columns: Option<Vec<ExprRef>>) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            stats_state: StatsState::NotMaterialized,
            columns,
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
        let stats = calculate_distinct_stats(input_stats);
        self.stats_state = StatsState::Materialized(stats.into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let distinct_label = if let Some(columns) = &self.columns {
            format!(
                "Distinct: On {}",
                columns
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            "Distinct".to_string()
        };

        let mut res = vec![distinct_label];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
