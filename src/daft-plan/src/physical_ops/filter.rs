use std::sync::Arc;

use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Filter {
    // The Boolean expression to filter on.
    pub predicate: Expr,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl Filter {
    pub(crate) fn new(predicate: Expr, input: Arc<PhysicalPlan>) -> Self {
        Self { predicate, input }
    }
}
