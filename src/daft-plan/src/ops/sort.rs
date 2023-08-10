use std::sync::Arc;

use daft_dsl::Expr;

use crate::LogicalPlan;

#[derive(Clone, Debug)]
pub struct Sort {
    pub sort_by: Vec<Expr>,
    pub descending: Vec<bool>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Sort {
    pub(crate) fn new(sort_by: Vec<Expr>, descending: Vec<bool>, input: Arc<LogicalPlan>) -> Self {
        Self {
            sort_by,
            descending,
            input,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("Sort:".to_string());
        if !self.sort_by.is_empty() {
            let pairs: Vec<String> = self
                .sort_by
                .iter()
                .zip(self.descending.iter())
                .map(|(sb, d)| {
                    format!(
                        "({:?}, {})",
                        sb,
                        if *d { "descending" } else { "ascending" },
                    )
                })
                .collect();
            res.push(format!("  Sort by: {:?}", pairs));
        }
        res
    }
}
