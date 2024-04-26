use daft_dsl::ExprRef;

use crate::physical_plan::PhysicalPlanRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Pivot {
    // Upstream node.
    pub input: PhysicalPlanRef,

    pub group_by: ExprRef,
    pub pivot_column: ExprRef,
    pub value_column: ExprRef,
}

impl Pivot {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        group_by: ExprRef,
        pivot_column: ExprRef,
        value_column: ExprRef,
    ) -> Self {
        Self {
            input,
            group_by,
            pivot_column,
            value_column,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("Pivot:".to_string());
        res.push(format!("Group by: {}", self.group_by));
        res.push(format!("Pivot column: {}", self.pivot_column));
        res.push(format!("Value column: {}", self.value_column));
        res
    }
}
