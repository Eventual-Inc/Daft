use daft_dsl::ExprRef;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TopN {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub sort_by: Vec<ExprRef>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub limit: u64,
    pub num_partitions: usize,
}

impl TopN {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        sort_by: Vec<ExprRef>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        limit: u64,
        num_partitions: usize,
    ) -> Self {
        Self {
            input,
            sort_by,
            descending,
            nulls_first,
            limit,
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
            .zip(self.nulls_first.iter())
            .map(|((sb, d), nf)| {
                format!(
                    "({}, {}, {})",
                    sb,
                    if *d { "descending" } else { "ascending" },
                    if *nf { "nulls first" } else { "nulls last" }
                )
            })
            .join(", ");
        res.push(format!(
            "TopN: Sort by = {}, Num Rows = {}",
            pairs, self.limit
        ));
        res.push(format!("Num partitions = {}", self.num_partitions));
        res
    }
}

crate::impl_default_tree_display!(TopN);
