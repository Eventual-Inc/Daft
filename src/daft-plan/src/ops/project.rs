use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_dsl::Expr;

use crate::LogicalPlan;

#[derive(Clone, Debug)]
pub struct Project {
    pub projection: Vec<Expr>,
    pub projected_schema: SchemaRef,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Project {
    pub(crate) fn new(
        projection: Vec<Expr>,
        projected_schema: SchemaRef,
        input: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            projection,
            projected_schema,
            input,
        }
    }
}
