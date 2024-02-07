use crate::physical_plan::PhysicalPlanRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Coalesce {
    // Upstream node.
    pub input: PhysicalPlanRef,

    // Number of partitions to coalesce from and to.
    pub num_from: usize,
    pub num_to: usize,
}

impl Coalesce {
    pub(crate) fn new(input: PhysicalPlanRef, num_from: usize, num_to: usize) -> Self {
        Self {
            input,
            num_from,
            num_to,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Coalesce: Num from = {}", self.num_from));
        res.push(format!("Num to = {}", self.num_to));
        res
    }
}
