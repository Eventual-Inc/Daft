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

        let agg_resolver = ExprResolver::builder().groupby(&groupby).build();
        let (aggregations, aggregation_fields) = agg_resolver
            .resolve(aggregations, &upstream_schema)
            .context(CreationSnafu)?;

        let groupby_resolver = ExprResolver::default();
        let (groupby, groupby_fields) = groupby_resolver
            .resolve(groupby, &upstream_schema)
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

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // TODO(desmond): We can use the schema here for better estimations. For now, use the old logic.
        let input_stats = self.input.materialized_stats();
        let est_bytes_per_row =
            input_stats.approx_stats.size_bytes / (input_stats.approx_stats.num_rows.max(1));
        let approx_stats = if self.groupby.is_empty() {
            ApproxStats {
                num_rows: 1,
                size_bytes: est_bytes_per_row,
            }
        } else {
            // TODO: Make a better estimation. Currently we assume that the groupby reduces the number of rows by 20%
            let est_num_groups = input_stats.approx_stats.num_rows / 5 * 4;
            ApproxStats {
                num_rows: est_num_groups,
                size_bytes: est_bytes_per_row * est_num_groups,
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
