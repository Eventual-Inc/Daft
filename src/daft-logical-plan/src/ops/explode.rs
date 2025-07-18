use std::sync::Arc;

use daft_dsl::{exprs_to_schema, ExprRef};
use daft_schema::schema::{Schema, SchemaRef};
use daft_stats::plan_stats::{calculate::calculate_explode_stats, StatsState};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    logical_plan::{self},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Explode {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Expressions to explode. e.g. col("a")
    pub to_explode: Vec<ExprRef>,
    pub exploded_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl Explode {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        to_explode: Vec<ExprRef>,
    ) -> logical_plan::Result<Self> {
        let exploded_schema = {
            let explode_exprs = to_explode
                .iter()
                .cloned()
                .map(daft_functions_list::explode)
                .collect::<Vec<_>>();

            let explode_schema = exprs_to_schema(&explode_exprs, input.schema())?;

            let input_schema = input.schema();
            let fields = input_schema
                .into_iter()
                .map(|field| explode_schema.get_field(&field.name).unwrap_or(field))
                .cloned();

            Schema::new(fields).into()
        };

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            to_explode,
            exploded_schema,
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
        let stats = calculate_explode_stats(input_stats);
        self.stats_state = StatsState::Materialized(stats.into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Explode: {}",
            self.to_explode.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Schema = {}", self.exploded_schema.short_string()));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
