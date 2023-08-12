use std::sync::Arc;

use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FanoutRandom {
    pub num_partitions: usize,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl FanoutRandom {
    pub(crate) fn new(num_partitions: usize, input: Arc<PhysicalPlan>) -> Self {
        Self {
            num_partitions,
            input,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FanoutByHash {
    pub num_partitions: usize,
    pub partition_by: Vec<Expr>,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl FanoutByHash {
    pub(crate) fn new(
        num_partitions: usize,
        partition_by: Vec<Expr>,
        input: Arc<PhysicalPlan>,
    ) -> Self {
        Self {
            num_partitions,
            partition_by,
            input,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FanoutByRange {
    pub num_partitions: usize,
    pub sort_by: Vec<Expr>,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl FanoutByRange {
    #[allow(dead_code)]
    pub(crate) fn new(num_partitions: usize, sort_by: Vec<Expr>, input: Arc<PhysicalPlan>) -> Self {
        Self {
            num_partitions,
            sort_by,
            input,
        }
    }
}
