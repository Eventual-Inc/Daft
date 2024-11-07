use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Distinct {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Distinct {
    pub(crate) fn new(input: Arc<LogicalPlan>) -> Self {
        Self { input }
    }
}
