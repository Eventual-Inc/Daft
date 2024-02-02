use std::sync::Arc;

use daft_dsl::Expr;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        // Must have at least one expression to sort by.
        assert!(!self.sort_by.is_empty());
        let pairs = self
            .sort_by
            .iter()
            .zip(self.descending.iter())
            .map(|(sb, d)| format!("({}, {})", sb, if *d { "descending" } else { "ascending" },))
            .collect::<Vec<_>>()
            .join(", ");
        res.push(format!("Sort: Sort by = {}", pairs));
        res.push(format!("Num partitions = {}", self.num_partitions));
        res
    }
}
