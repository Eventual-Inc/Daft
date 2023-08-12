use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Split {
    pub input_num_partitions: usize,
    pub output_num_partitions: usize,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl Split {
    pub(crate) fn new(
        input_num_partitions: usize,
        output_num_partitions: usize,
        input: Arc<PhysicalPlan>,
    ) -> Self {
        Self {
            input_num_partitions,
            output_num_partitions,
            input,
        }
    }
}
