use std::sync::Arc;

use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
}
