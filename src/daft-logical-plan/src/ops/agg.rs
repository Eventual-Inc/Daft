use std::sync::Arc;

use daft_dsl::{exprs_to_schema, ExprRef};
use daft_schema::schema::SchemaRef;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    logical_plan::{self},
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Aggregate {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,

    /// Aggregations to apply.
    ///
    /// Initially, the root level expressions may not be aggregations,
    /// but they should be factored out into a project by an optimization rule,
    /// leaving only aliases and agg expressions by translation time.
    pub aggregations: Vec<ExprRef>,

    /// Grouping to apply.
    pub groupby: Vec<ExprRef>,

    pub output_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl Aggregate {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        aggregations: Vec<ExprRef>,
        groupby: Vec<ExprRef>,
    ) -> logical_plan::Result<Self> {
        let output_schema = exprs_to_schema(
            &[groupby.as_slice(), aggregations.as_slice()].concat(),
            input.schema(),
        )?;

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            aggregations,
            groupby,
            output_schema,
            stats_state: StatsState::NotMaterialized,
        })
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
        // TODO(desmond): We can use the schema here for better estimations. For now, use the old logic.
        let input_stats = self.input.materialized_stats();
        let est_bytes_per_row =
            input_stats.approx_stats.size_bytes / (input_stats.approx_stats.num_rows.max(1));
        let acc_selectivity = if input_stats.approx_stats.num_rows == 0 {
            0.0
        } else {
            input_stats.approx_stats.acc_selectivity / input_stats.approx_stats.num_rows as f64
        };
        let approx_stats = if self.groupby.is_empty() {
            ApproxStats {
                num_rows: 1,
                size_bytes: est_bytes_per_row,
                acc_selectivity,
            }
        } else {
            // Assume high cardinality for group by columns, and 80% of rows are unique.
            let est_num_groups = input_stats.approx_stats.num_rows * 4 / 5;
            ApproxStats {
                num_rows: est_num_groups,
                size_bytes: est_bytes_per_row * est_num_groups,
                acc_selectivity: input_stats.approx_stats.acc_selectivity * est_num_groups as f64
                    / input_stats.approx_stats.num_rows as f64,
            }
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Aggregation: {}",
            self.aggregations.iter().map(|e| e.to_string()).join(", ")
        ));
        if !self.groupby.is_empty() {
            res.push(format!(
                "Group by = {}",
                self.groupby.iter().map(|e| e.to_string()).join(", ")
            ));
        }
        res.push(format!(
            "Output schema = {}",
            self.output_schema.short_string()
        ));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
