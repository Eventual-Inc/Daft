use daft_dsl::ExprRef;
use itertools::Itertools;

use crate::physical_plan::PhysicalPlanRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FanoutRandom {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub num_partitions: usize,
}

impl FanoutRandom {
    pub(crate) fn new(input: PhysicalPlanRef, num_partitions: usize) -> Self {
        Self {
            input,
            num_partitions,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("FanoutRandom: {}", self.num_partitions)]
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FanoutByHash {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub num_partitions: usize,
    pub partition_by: Vec<ExprRef>,
}

impl FanoutByHash {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        num_partitions: usize,
        partition_by: Vec<ExprRef>,
    ) -> Self {
        Self {
            input,
            num_partitions,
            partition_by,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("FanoutByHash: {}", self.num_partitions));
        res.push(format!(
            "Partition by = {}",
            self.partition_by.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FanoutByRange {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub num_partitions: usize,
    pub sort_by: Vec<ExprRef>,
    pub descending: Vec<bool>,
}

impl FanoutByRange {
    #[allow(dead_code)]
    pub(crate) fn new(
        input: PhysicalPlanRef,
        num_partitions: usize,
        sort_by: Vec<ExprRef>,
        descending: Vec<bool>,
    ) -> Self {
        Self {
            input,
            num_partitions,
            sort_by,
            descending,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("FanoutByRange: {}", self.num_partitions));
        let pairs = self
            .sort_by
            .iter()
            .zip(self.descending.iter())
            .map(|(sb, d)| format!("({}, {})", sb, if *d { "descending" } else { "ascending" },))
            .join(", ");
        res.push(format!("Sort by = {}", pairs,));
        res
    }
}
