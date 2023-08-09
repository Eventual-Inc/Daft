use std::sync::Arc;

use daft_dsl::{AggExpr, Expr};

use crate::physical_plan::PhysicalPlan;

#[derive(Clone, Debug)]
pub struct Aggregate {
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
    ) -> Self {
        Self {
            aggregations,
            group_by,
            input,
        }
    }
}
