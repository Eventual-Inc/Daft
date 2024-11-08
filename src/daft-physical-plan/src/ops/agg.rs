use daft_dsl::{AggExpr, ExprRef};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{impl_default_tree_display, PhysicalPlanRef};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Aggregate {
    // Upstream node.
    pub input: PhysicalPlanRef,

    /// Aggregations to apply.
    pub aggregations: Vec<AggExpr>,

    /// Grouping to apply.
    pub groupby: Vec<ExprRef>,
}

impl Aggregate {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        aggregations: Vec<AggExpr>,
        groupby: Vec<ExprRef>,
    ) -> Self {
        Self {
            input,
            aggregations,
            groupby,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Aggregation: {}",
            self.aggregations.iter().map(|e| e.to_string()).join(", ")
        ));
        if !self.groupby.is_empty() {
            res.push(format!(
                "Group by = {}",
                self.groupby.iter().map(|e| e.to_string()).join(", ")
            ));
        }
        res
    }
}

impl_default_tree_display!(Aggregate);
