use std::sync::Arc;

use daft_dsl::ExprRef;

use crate::LogicalPlan;

#[derive(Clone, Debug)]
pub struct Filter {
    // The Boolean expression to filter on.
    pub predicate: ExprRef,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Filter {
    pub(crate) fn new(predicate: ExprRef, input: Arc<LogicalPlan>) -> Self {
        Self { predicate, input }
    }
}
