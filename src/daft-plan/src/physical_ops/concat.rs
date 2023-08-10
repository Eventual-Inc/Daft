use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;

#[derive(Clone, Debug)]
pub struct Concat {
    pub other: Arc<PhysicalPlan>,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl Concat {
    pub(crate) fn new(other: Arc<PhysicalPlan>, input: Arc<PhysicalPlan>) -> Self {
        Self { other, input }
    }
}
