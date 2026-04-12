use std::sync::Arc;

use daft_dsl::ExprRef;
use serde::{Deserialize, Serialize};

use crate::{
    LogicalPlan,
    stats::{ApproxStats, PlanStats, StatsState},
};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
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
        // Assume high cardinality, 90% of the values are distinct in every column.
        const APPROX_COLUMN_NDV_SELECTIVITY: f64 = 0.8;
        let num_columns = if let Some(columns) = &self.columns {
            columns.len()
        } else {
            self.input.schema().len()
        };
        let approx_column_ndv = 1.0 - (1.0 - APPROX_COLUMN_NDV_SELECTIVITY) * (num_columns as f64);

        // TODO(desmond): We can simply use NDVs here. For now, do a naive estimation.
        let input_stats = self.input.materialized_stats();
        let est_bytes_per_row =
            input_stats.approx_stats.size_bytes / (input_stats.approx_stats.num_rows.max(1));
        let est_distinct_values = ((input_stats.approx_stats.num_rows as f64) * approx_column_ndv) as usize;
        let acc_selectivity = if input_stats.approx_stats.num_rows == 0 {
            0.0
        } else {
            input_stats.approx_stats.acc_selectivity * approx_column_ndv
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
