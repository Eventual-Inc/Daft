use std::sync::Arc;

use crate::{
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Distinct {
    pub plan_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub stats_state: StatsState,
}

impl Distinct {
    pub(crate) fn new(input: Arc<LogicalPlan>) -> Self {
        Self {
            plan_id: None,
            input,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // TODO(desmond): We can simply use NDVs here. For now, do a naive estimation.
        let input_stats = self.input.materialized_stats();
        let est_bytes_per_row =
            input_stats.approx_stats.size_bytes / (input_stats.approx_stats.num_rows.max(1));
        // Assume high cardinality, 80% of rows are distinct.
        let est_distinct_values = input_stats.approx_stats.num_rows * 4 / 5;
        let acc_selectivity = if input_stats.approx_stats.num_rows == 0 {
            0.0
        } else {
            input_stats.approx_stats.acc_selectivity * est_distinct_values as f64
                / input_stats.approx_stats.num_rows as f64
        };
        let approx_stats = ApproxStats {
            num_rows: est_distinct_values,
            size_bytes: est_distinct_values * est_bytes_per_row,
            acc_selectivity,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("Distinct")];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
