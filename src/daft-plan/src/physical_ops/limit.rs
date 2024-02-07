use crate::physical_plan::PhysicalPlanRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limit {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub limit: i64,
    pub eager: bool,
    pub num_partitions: usize,
}

impl Limit {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        limit: i64,
        eager: bool,
        num_partitions: usize,
    ) -> Self {
        Self {
            input,
            limit,
            eager,
            num_partitions,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Limit: {}", self.limit));
        res.push(format!("Eager = {}", self.eager));
        res.push(format!("Num partitions = {}", self.num_partitions));
        res
    }
}
