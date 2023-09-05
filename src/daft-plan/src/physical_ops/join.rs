use std::sync::Arc;

use daft_dsl::Expr;

use crate::{physical_plan::PhysicalPlan, JoinType};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Join {
    // Upstream node.
    pub left: Arc<PhysicalPlan>,
    pub right: Arc<PhysicalPlan>,
    pub left_on: Vec<Expr>,
    pub right_on: Vec<Expr>,
    pub join_type: JoinType,
}

impl Join {
    pub(crate) fn new(
        left: Arc<PhysicalPlan>,
        right: Arc<PhysicalPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        join_type: JoinType,
    ) -> Self {
        Self {
            left,
            right,
            left_on,
            right_on,
            join_type,
        }
    }
}
