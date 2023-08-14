use std::sync::Arc;

use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Explode {
    pub explode_exprs: Vec<Expr>,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl Explode {
    pub(crate) fn new(explode_exprs: Vec<Expr>, input: Arc<PhysicalPlan>) -> Self {
        Self {
            explode_exprs,
            input,
        }
    }
}
