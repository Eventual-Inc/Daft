use std::sync::Arc;

use daft_dsl::Expr;

use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Filter {
    // The Boolean expression to filter on.
    pub predicate: Expr,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Filter {
    pub(crate) fn new(predicate: Expr, input: Arc<LogicalPlan>) -> Self {
        Self { predicate, input }
    }
}
