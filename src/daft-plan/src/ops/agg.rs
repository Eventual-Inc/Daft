use std::sync::Arc;

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::{AggExpr, Expr};

use crate::LogicalPlan;

#[derive(Clone, Debug)]
pub struct Aggregate {
    /// Aggregations to apply.
    pub aggregations: Vec<AggExpr>,

    /// Grouping to apply.
    pub group_by: Vec<Expr>,

    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Aggregate {
    pub(crate) fn new(aggregations: Vec<AggExpr>, input: Arc<LogicalPlan>) -> Self {
        // TEMP: No groupbys supported for now.
        let group_by: Vec<Expr> = vec![];

        Self {
            aggregations,
            group_by,
            input,
        }
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        let source_schema = self.input.schema();

        let fields = self
            .group_by
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
        if !self.group_by.is_empty() {
            res.push(format!("  Group by: {:?}", self.group_by));
        }
        res.push(format!("  Output schema: {}", self.schema().short_string()));
        res
    }
}
