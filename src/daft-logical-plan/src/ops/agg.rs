use std::sync::Arc;

use daft_dsl::{exprs_to_schema, ExprRef};
use daft_schema::schema::SchemaRef;
use daft_stats::plan_stats::{
    calculate::{calculate_hash_aggregate_stats, calculate_ungrouped_aggregate_stats},
    StatsState,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    logical_plan::{self},
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
        let input_stats = self.input.materialized_stats();
        let stats = if self.groupby.is_empty() {
            calculate_ungrouped_aggregate_stats(input_stats)
        } else {
            calculate_hash_aggregate_stats(
                input_stats,
                &self.groupby.iter().map(|e| e.as_ref()).collect::<Vec<_>>(),
            )
        };
        self.stats_state = StatsState::Materialized(stats.into());
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
