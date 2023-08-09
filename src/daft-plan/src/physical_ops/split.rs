use std::sync::Arc;

use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlan;

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct SplitRandom {
    pub num_partitions: usize,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl SplitRandom {
    pub(crate) fn new(num_partitions: usize, input: Arc<PhysicalPlan>) -> Self {
        Self {
            num_partitions,
            input,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SplitByHash {
    pub num_partitions: usize,
    pub partition_by: Vec<Expr>,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl SplitByHash {
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

#[derive(Clone, Debug)]
pub struct SplitByRange {
    pub num_partitions: usize,
    pub sort_by: Vec<Expr>,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl SplitByRange {
    #[allow(dead_code)]
    pub(crate) fn new(num_partitions: usize, sort_by: Vec<Expr>, input: Arc<PhysicalPlan>) -> Self {
        Self {
            num_partitions,
            sort_by,
            input,
        }
    }
}
