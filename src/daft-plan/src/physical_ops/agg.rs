use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_dsl::{AggExpr, Expr};

use crate::physical_plan::PhysicalPlan;

#[derive(Clone, Debug)]
pub struct Aggregate {
    /// The schema of the output of this node.
    pub schema: SchemaRef,

    /// Aggregations to apply.
    pub aggregations: Vec<AggExpr>,

    /// Grouping to apply.
    pub group_by: Vec<Expr>,

    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl Aggregate {
    pub(crate) fn new(
        input: Arc<PhysicalPlan>,
        aggregations: Vec<AggExpr>,
        group_by: Vec<Expr>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            schema,
            aggregations,
            group_by,
            input,
        }
    }
}
