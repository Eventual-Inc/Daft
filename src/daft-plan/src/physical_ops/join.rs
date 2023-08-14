use std::sync::Arc;

use daft_dsl::Expr;

use crate::{physical_plan::PhysicalPlan, JoinType};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Join {
    pub right: Arc<PhysicalPlan>,
    pub left_on: Vec<Expr>,
    pub right_on: Vec<Expr>,
    pub output_projection: Vec<Expr>,
    pub join_type: JoinType,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl Join {
    pub(crate) fn new(
        right: Arc<PhysicalPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        output_projection: Vec<Expr>,
        join_type: JoinType,
        input: Arc<PhysicalPlan>,
    ) -> Self {
        Self {
            right,
            left_on,
            right_on,
            output_projection,
            join_type,
            input,
        }
    }
}
