use std::sync::Arc;

use daft_dsl::{exprs_to_schema, ExprRef};
use daft_schema::schema::{Schema, SchemaRef};
use itertools::Itertools;

use crate::{
    logical_plan::{self},
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Explode {
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
                .map(daft_functions::list::explode)
                .collect::<Vec<_>>();

            let explode_schema = exprs_to_schema(&explode_exprs, input.schema())?;

            let fields = input
                .schema()
                .fields
                .iter()
                .map(|(name, field)| explode_schema.fields.get(name).unwrap_or(field))
                .cloned()
                .collect::<Vec<_>>();
            Schema::new(fields)?.into()
        };

        Ok(Self {
            input,
            to_explode,
            exploded_schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let input_stats = self.input.materialized_stats();
        let est_num_exploded_rows = input_stats.approx_stats.num_rows * 4;
        let acc_selectivity = if input_stats.approx_stats.num_rows == 0 {
            0.0
        } else {
            input_stats.approx_stats.acc_selectivity * est_num_exploded_rows as f64
                / input_stats.approx_stats.num_rows as f64
        };
        let approx_stats = ApproxStats {
            num_rows: est_num_exploded_rows,
            size_bytes: input_stats.approx_stats.size_bytes,
            acc_selectivity,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
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
