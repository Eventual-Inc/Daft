use std::sync::Arc;

use daft_dsl::{ExprRef, ExprResolver};
use daft_schema::schema::{Schema, SchemaRef};
use itertools::Itertools;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
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
        let upstream_schema = input.schema();

        let expr_resolver = ExprResolver::default();

        let (to_explode, _) = expr_resolver
            .resolve(to_explode, &upstream_schema)
            .context(CreationSnafu)?;

        let explode_exprs = to_explode
            .iter()
            .cloned()
            .map(daft_functions::list::explode)
            .collect::<Vec<_>>();
        let exploded_schema = {
            let explode_schema = {
                let explode_fields = explode_exprs
                    .iter()
                    .map(|e| e.to_field(&upstream_schema))
                    .collect::<common_error::DaftResult<Vec<_>>>()
                    .context(CreationSnafu)?;
                Schema::new(explode_fields).context(CreationSnafu)?
            };
            let fields = upstream_schema
                .fields
                .iter()
                .map(|(name, field)| explode_schema.fields.get(name).unwrap_or(field))
                .cloned()
                .collect::<Vec<_>>();
            Schema::new(fields).context(CreationSnafu)?.into()
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
        let approx_stats = ApproxStats {
            lower_bound_rows: input_stats.approx_stats.lower_bound_rows,
            upper_bound_rows: None,
            lower_bound_bytes: input_stats.approx_stats.lower_bound_bytes,
            upper_bound_bytes: None,
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
