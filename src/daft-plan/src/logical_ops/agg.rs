use std::sync::Arc;

use snafu::ResultExt;

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::{AggExpr, Expr};

use crate::logical_plan::{self, CreationSnafu};
use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Aggregate {
    // Upstream node.
    pub input: Arc<LogicalPlan>,

    /// Aggregations to apply.
    pub aggregations: Vec<AggExpr>,

    /// Grouping to apply.
    pub groupby: Vec<Expr>,

    pub output_schema: SchemaRef,
}

impl Aggregate {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        aggregations: Vec<AggExpr>,
        groupby: Vec<Expr>,
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

    pub(crate) fn schema(&self) -> SchemaRef {
        let source_schema = self.input.schema();

        let fields = self
            .groupby
            .iter()
            .map(|expr| expr.to_field(&source_schema).unwrap())
            .chain(
                self.aggregations
                    .iter()
                    .map(|agg_expr| agg_expr.to_field(&source_schema).unwrap()),
            )
            .collect();
        Schema::new(fields).unwrap().into()
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Aggregation: {}",
            self.aggregations
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        if !self.groupby.is_empty() {
            res.push(format!(
                "Group by = {}",
                self.groupby
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        res.push(format!("Output schema = {}", self.schema().short_string()));
        res
    }
}
