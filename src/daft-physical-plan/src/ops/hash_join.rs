use common_display::tree::TreeDisplay;
use daft_core::join::JoinType;
use daft_dsl::ExprRef;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HashJoin {
    // Upstream node.
    pub left: PhysicalPlanRef,
    pub right: PhysicalPlanRef,
    pub left_on: Vec<ExprRef>,
    pub right_on: Vec<ExprRef>,
    pub null_equals_nulls: Option<Vec<bool>>,
    pub join_type: JoinType,
}

impl HashJoin {
    pub(crate) fn new(
        left: PhysicalPlanRef,
        right: PhysicalPlanRef,
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        null_equals_nulls: Option<Vec<bool>>,
        join_type: JoinType,
    ) -> Self {
        Self {
            left,
            right,
            left_on,
            right_on,
            null_equals_nulls,
            join_type,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("HashJoin: Type = {}", self.join_type));
        if !self.left_on.is_empty() && !self.right_on.is_empty() && self.left_on == self.right_on {
            res.push(format!(
                "On = {}",
                self.left_on.iter().map(|e| e.to_string()).join(", ")
            ));
        } else {
            if !self.left_on.is_empty() {
                res.push(format!(
                    "Left on = {}",
                    self.left_on.iter().map(|e| e.to_string()).join(", ")
                ));
            }
            if !self.right_on.is_empty() {
                res.push(format!(
                    "Right on = {}",
                    self.right_on.iter().map(|e| e.to_string()).join(", ")
                ));
            }
        }
        if let Some(null_equals_nulls) = &self.null_equals_nulls {
            res.push(format!(
                "Null equals Nulls = [{}]",
                null_equals_nulls.iter().map(|b| b.to_string()).join(", ")
            ));
        }
        res
    }
}

impl TreeDisplay for HashJoin {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        match level {
            common_display::DisplayLevel::Compact => self.get_name(),
            _ => self.multiline_display().join("\n"),
        }
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }
}
