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
pub struct Aggregate {
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
        let upstream_schema = input.schema();

        let groupby_resolver = ExprResolver::default();
        let agg_resolver = ExprResolver::builder().in_agg_context(true).build();

        let (groupby, groupby_fields) = groupby_resolver
            .resolve(groupby, &upstream_schema)
            .context(CreationSnafu)?;
        let (aggregations, aggregation_fields) = agg_resolver
            .resolve(aggregations, &upstream_schema)
            .context(CreationSnafu)?;

        let fields = [groupby_fields, aggregation_fields].concat();

        let output_schema = Schema::new(fields).context(CreationSnafu)?.into();

        Ok(Self {
            input,
            aggregations,
            groupby,
            output_schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub(crate) fn materialize_stats(&self) -> Self {
        // TODO(desmond): We can use the schema here for better estimations. For now, use the old logic.
        let new_input = self.input.materialize_stats();
        let input_stats = new_input.get_stats();
        let est_bytes_per_row_lower = input_stats.approx_stats.lower_bound_bytes
            / (input_stats.approx_stats.lower_bound_rows.max(1));
        let est_bytes_per_row_upper =
            input_stats
                .approx_stats
                .upper_bound_bytes
                .and_then(|bytes| {
                    input_stats
                        .approx_stats
                        .upper_bound_rows
                        .map(|rows| bytes / rows.max(1))
                });
        let approx_stats = if self.groupby.is_empty() {
            ApproxStats {
                lower_bound_rows: input_stats.approx_stats.lower_bound_rows.min(1),
                upper_bound_rows: Some(1),
                lower_bound_bytes: input_stats.approx_stats.lower_bound_bytes.min(1)
                    * est_bytes_per_row_lower,
                upper_bound_bytes: est_bytes_per_row_upper,
            }
        } else {
            ApproxStats {
                lower_bound_rows: input_stats.approx_stats.lower_bound_rows.min(1),
                upper_bound_rows: input_stats.approx_stats.upper_bound_rows,
                lower_bound_bytes: input_stats.approx_stats.lower_bound_bytes.min(1)
                    * est_bytes_per_row_lower,
                upper_bound_bytes: input_stats.approx_stats.upper_bound_bytes,
            }
        };
        let stats_state = StatsState::Materialized(PlanStats::new(approx_stats));
        Self {
            input: Arc::new(new_input),
            aggregations: self.aggregations.clone(),
            groupby: self.groupby.clone(),
            output_schema: self.output_schema.clone(),
            stats_state,
        }
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
        res
    }
}
