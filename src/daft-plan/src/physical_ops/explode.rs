use std::sync::Arc;

use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Explode {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
    pub to_explode: Vec<Expr>,
}

impl Explode {
    pub(crate) fn new(input: Arc<PhysicalPlan>, to_explode: Vec<Expr>) -> Self {
        Self { input, to_explode }
    }
}
