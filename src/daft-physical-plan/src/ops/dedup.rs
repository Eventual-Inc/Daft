use daft_dsl::ExprRef;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{impl_default_tree_display, PhysicalPlanRef};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Dedup {
    // Upstream node.
    pub input: PhysicalPlanRef,

    /// Aggregations to apply.
    pub columns: Vec<ExprRef>,
}

impl Dedup {
    pub(crate) fn new(input: PhysicalPlanRef, columns: Vec<ExprRef>) -> Self {
        Self { input, columns }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Dedup: {}",
            self.columns.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }
}

impl_default_tree_display!(Dedup);
