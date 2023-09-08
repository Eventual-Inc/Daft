use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Concat {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
    pub other: Arc<PhysicalPlan>,
}

impl Concat {
    pub(crate) fn new(input: Arc<PhysicalPlan>, other: Arc<PhysicalPlan>) -> Self {
        Self { input, other }
    }
}
