use std::sync::Arc;

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::{AggExpr, Expr};

use crate::LogicalPlan;

#[derive(Clone, Debug)]
pub struct Aggregate {
    /// Aggregations to apply.
    pub aggregations: Vec<AggExpr>,

    /// Grouping to apply.
    pub groupby: Vec<Expr>,

    pub output_schema: SchemaRef,

    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Aggregate {
    pub(crate) fn new(
        aggregations: Vec<AggExpr>,
        groupby: Vec<Expr>,
        output_schema: SchemaRef,
        input: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            aggregations,
            groupby,
            output_schema,
            input,
        }
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
        res.push(format!("Aggregation: {:?}", self.aggregations));
        if !self.groupby.is_empty() {
            res.push(format!("  Group by: {:?}", self.groupby));
        }
        res.push(format!("  Output schema: {}", self.schema().short_string()));
        res
    }
}
