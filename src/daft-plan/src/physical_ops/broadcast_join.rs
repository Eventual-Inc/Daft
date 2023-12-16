use std::sync::Arc;

use daft_dsl::Expr;

use crate::{physical_plan::PhysicalPlan, JoinType};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BroadcastJoin {
    // Upstream node.
    pub broadcaster: Arc<PhysicalPlan>,
    pub receiver: Arc<PhysicalPlan>,
    pub left_on: Vec<Expr>,
    pub right_on: Vec<Expr>,
    pub join_type: JoinType,
    pub is_swapped: bool,
}

impl BroadcastJoin {
    pub(crate) fn new(
        broadcaster: Arc<PhysicalPlan>,
        receiver: Arc<PhysicalPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        join_type: JoinType,
        is_swapped: bool,
    ) -> Self {
        Self {
            broadcaster,
            receiver,
            left_on,
            right_on,
            join_type,
            is_swapped,
        }
    }
}
