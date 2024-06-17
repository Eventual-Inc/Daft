use std::sync::Arc;

use itertools::Itertools;
use snafu::ResultExt;

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::{resolve_aggexprs, resolve_exprs, AggExpr, ExprRef};

use crate::logical_plan::{self, CreationSnafu};
use crate::LogicalPlan;

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
            resolve_exprs(groupby, &upstream_schema).context(CreationSnafu)?;
        let (aggregations, aggregation_fields) =
            resolve_aggexprs(aggregations, &upstream_schema).context(CreationSnafu)?;

        let fields = [groupby_fields, aggregation_fields].concat();

        let output_schema = Schema::new(fields).context(CreationSnafu)?.into();

        Ok(Self {
            aggregations,
            groupby,
            output_schema,
            input,
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
