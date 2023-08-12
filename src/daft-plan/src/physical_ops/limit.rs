use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Limit {
    pub limit: i64,
    pub num_partitions: usize,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl Limit {
    pub(crate) fn new(limit: i64, num_partitions: usize, input: Arc<PhysicalPlan>) -> Self {
        Self {
            limit,
            num_partitions,
            input,
        }
    }
}
