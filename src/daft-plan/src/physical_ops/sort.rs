use daft_dsl::ExprRef;
use itertools::Itertools;

use crate::physical_plan::PhysicalPlanRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sort {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub sort_by: Vec<ExprRef>,
    pub descending: Vec<bool>,
    pub num_partitions: usize,
}

impl Sort {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        sort_by: Vec<ExprRef>,
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
            .join(", ");
        res.push(format!("Sort: Sort by = {}", pairs));
        res.push(format!("Num partitions = {}", self.num_partitions));
        res
    }
}
