use std::sync::Arc;

use daft_dsl::functions::prelude::{Deserialize, Serialize};

use crate::{stats::StatsState, LogicalPlan};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Offset {
    pub plan_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Offset on number of rows.
    pub offset: i64,
    pub stats_state: StatsState,
}

impl Offset {
    pub(crate) fn new(input: Arc<LogicalPlan>, offset: i64) -> Self {
        Self {
            plan_id: None,
            input,
            offset,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // TODO(zhenchao) For now, reuse the old logic.
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
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
