use std::sync::Arc;

use itertools::Itertools;
use snafu::ResultExt;

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::{AggExpr, ExprRef};

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
        aggregations: Vec<AggExpr>,
        groupby: Vec<ExprRef>,
    ) -> logical_plan::Result<Self> {
        let output_schema = {
            let upstream_schema = input.schema();
            let fields = groupby
                .iter()
                .map(|e| e.to_field(&upstream_schema))
                .chain(aggregations.iter().map(|ae| ae.to_field(&upstream_schema)))
                .collect::<common_error::DaftResult<Vec<_>>>()
                .context(CreationSnafu)?;
            Schema::new(fields).context(CreationSnafu)?.into()
        };

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
