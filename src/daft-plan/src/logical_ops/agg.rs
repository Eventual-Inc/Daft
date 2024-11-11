use std::sync::Arc;

use daft_dsl::{resolve_aggexprs, resolve_exprs, AggExpr, ExprRef};
use daft_schema::schema::{Schema, SchemaRef};
use itertools::Itertools;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Aggregate {
    // Upstream node.
    pub input: Arc<LogicalPlan>,

    /// Aggregations to apply.
    pub aggregations: Vec<AggExpr>,

    /// Grouping to apply.
    pub groupby: Vec<ExprRef>,

    pub output_schema: SchemaRef,
}

impl Aggregate {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        aggregations: Vec<ExprRef>,
        groupby: Vec<ExprRef>,
    ) -> logical_plan::Result<Self> {
        let upstream_schema = input.schema();
        let (groupby, groupby_fields) =
            resolve_exprs(groupby, &upstream_schema, false).context(CreationSnafu)?;
        let (aggregations, aggregation_fields) =
            resolve_aggexprs(aggregations, &upstream_schema).context(CreationSnafu)?;

        let fields = [groupby_fields, aggregation_fields].concat();

        let output_schema = Schema::new(fields).context(CreationSnafu)?.into();

        Ok(Self {
            input,
            aggregations,
            groupby,
            output_schema,
        })
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

use crate::stats::{ApproxStats, Stats};
impl Stats for Aggregate {
    fn approximate_stats(&self) -> ApproxStats {
        let input_stats = self.input.approximate_stats();
        // TODO(desmond): We can use the schema here for better estimations. For now, use the old logic.
        let est_bytes_per_row_lower =
            input_stats.lower_bound_bytes / (input_stats.lower_bound_rows.max(1));
        let est_bytes_per_row_upper = input_stats
            .upper_bound_bytes
            .and_then(|bytes| input_stats.upper_bound_rows.map(|rows| bytes / rows.max(1)));
        if self.groupby.is_empty() {
            ApproxStats {
                lower_bound_rows: input_stats.lower_bound_rows.min(1),
                upper_bound_rows: Some(1),
                lower_bound_bytes: input_stats.lower_bound_bytes.min(1) * est_bytes_per_row_lower,
                upper_bound_bytes: est_bytes_per_row_upper,
            }
        } else {
            ApproxStats {
                lower_bound_rows: input_stats.lower_bound_rows.min(1),
                upper_bound_rows: input_stats.upper_bound_rows,
                lower_bound_bytes: input_stats.lower_bound_bytes.min(1) * est_bytes_per_row_lower,
                upper_bound_bytes: input_stats.upper_bound_bytes,
            }
        }
    }
}
