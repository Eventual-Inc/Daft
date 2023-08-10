use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;

#[derive(Clone, Debug)]
pub struct ReduceMerge {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl ReduceMerge {
    pub(crate) fn new(input: Arc<PhysicalPlan>) -> Self {
        Self { input }
    }
}
