use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
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
