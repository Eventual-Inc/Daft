use std::sync::Arc;

use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sort {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
    pub sort_by: Vec<Expr>,
    pub descending: Vec<bool>,
    pub num_partitions: usize,
}

impl Sort {
    pub(crate) fn new(
        input: Arc<PhysicalPlan>,
        sort_by: Vec<Expr>,
        descending: Vec<bool>,
        num_partitions: usize,
    ) -> Self {
        Self {
            input,
            sort_by,
            descending,
            num_partitions,
        }
    }
}
