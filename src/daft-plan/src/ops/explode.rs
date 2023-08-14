use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_dsl::Expr;

use crate::LogicalPlan;

#[derive(Clone, Debug)]
pub struct Explode {
    pub explode_exprs: Vec<Expr>,
    pub exploded_schema: SchemaRef,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Explode {
    pub(crate) fn new(
        explode_exprs: Vec<Expr>,
        exploded_schema: SchemaRef,
        input: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            explode_exprs,
            exploded_schema,
            input,
        }
    }
}
