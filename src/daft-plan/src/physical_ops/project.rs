use std::sync::Arc;

use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub projection: Vec<Expr>,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl Project {
    pub(crate) fn new(projection: Vec<Expr>, input: Arc<PhysicalPlan>) -> Self {
        Self { projection, input }
    }
}
