use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Split {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
    pub input_num_partitions: usize,
    pub output_num_partitions: usize,
}

impl Split {
    pub(crate) fn new(
        input: Arc<PhysicalPlan>,
        input_num_partitions: usize,
        output_num_partitions: usize,
    ) -> Self {
        Self {
            input,
            input_num_partitions,
            output_num_partitions,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Split: Input num partitions = {}",
            self.input_num_partitions
        ));
        res.push(format!(
            "Output num partitions = {}",
            self.output_num_partitions
        ));
        res
    }
}
