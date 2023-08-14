use std::sync::Arc;

use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sort {
    pub sort_by: Vec<Expr>,
    pub descending: Vec<bool>,
    pub num_partitions: usize,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl Sort {
    pub(crate) fn new(
        sort_by: Vec<Expr>,
        descending: Vec<bool>,
        num_partitions: usize,
        input: Arc<PhysicalPlan>,
    ) -> Self {
        Self {
            sort_by,
            descending,
            num_partitions,
            input,
        }
    }
}
