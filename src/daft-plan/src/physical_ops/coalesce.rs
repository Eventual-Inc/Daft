use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Coalesce {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,

    // Number of partitions to coalesce from and to.
    pub num_from: usize,
    pub num_to: usize,
}

impl Coalesce {
    pub(crate) fn new(input: Arc<PhysicalPlan>, num_from: usize, num_to: usize) -> Self {
        Self {
            input,
            num_from,
            num_to,
        }
    }
}
