use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Limit {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
    pub limit: i64,
    pub num_partitions: usize,
}

impl Limit {
    pub(crate) fn new(input: Arc<PhysicalPlan>, limit: i64, num_partitions: usize) -> Self {
        Self {
            input,
            limit,
            num_partitions,
        }
    }
}
