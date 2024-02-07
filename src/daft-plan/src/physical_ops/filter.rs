use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlanRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Filter {
    // Upstream node.
    pub input: PhysicalPlanRef,
    // The Boolean expression to filter on.
    pub predicate: Expr,
}

impl Filter {
    pub(crate) fn new(input: PhysicalPlanRef, predicate: Expr) -> Self {
        Self { input, predicate }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Filter: {}", self.predicate));
        res
    }
}
