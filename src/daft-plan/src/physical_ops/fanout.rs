use std::sync::Arc;

use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FanoutRandom {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
    pub num_partitions: usize,
}

impl FanoutRandom {
    pub(crate) fn new(input: Arc<PhysicalPlan>, num_partitions: usize) -> Self {
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
    pub input: Arc<PhysicalPlan>,
    pub num_partitions: usize,
    pub partition_by: Vec<Expr>,
}

impl FanoutByHash {
    pub(crate) fn new(
        input: Arc<PhysicalPlan>,
        num_partitions: usize,
        partition_by: Vec<Expr>,
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
            self.partition_by
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        res
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FanoutByRange {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
    pub num_partitions: usize,
    pub sort_by: Vec<Expr>,
}

impl FanoutByRange {
    #[allow(dead_code)]
    pub(crate) fn new(input: Arc<PhysicalPlan>, num_partitions: usize, sort_by: Vec<Expr>) -> Self {
        Self {
            input,
            num_partitions,
            sort_by,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("FanoutByRange: {}", self.num_partitions));
        res.push(format!(
            "Sort by = {}",
            self.sort_by
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        res
    }
}
