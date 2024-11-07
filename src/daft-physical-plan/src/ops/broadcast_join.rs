use common_display::tree::TreeDisplay;
use daft_core::join::JoinType;
use daft_dsl::ExprRef;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BroadcastJoin {
    // Upstream node.
    pub broadcaster: PhysicalPlanRef,
    pub receiver: PhysicalPlanRef,
    pub left_on: Vec<ExprRef>,
    pub right_on: Vec<ExprRef>,
    pub null_equals_nulls: Option<Vec<bool>>,
    pub join_type: JoinType,
    pub is_swapped: bool,
}

impl BroadcastJoin {
    pub(crate) fn new(
        broadcaster: PhysicalPlanRef,
        receiver: PhysicalPlanRef,
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        null_equals_nulls: Option<Vec<bool>>,
        join_type: JoinType,
        is_swapped: bool,
    ) -> Self {
        Self {
            broadcaster,
            receiver,
            left_on,
            right_on,
            null_equals_nulls,
            join_type,
            is_swapped,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("BroadcastJoin: Type = {}", self.join_type));
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
        res.push(format!("Is swapped = {}", self.is_swapped));
        res
    }
}

impl TreeDisplay for BroadcastJoin {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        match level {
            common_display::DisplayLevel::Compact => self.get_name(),
            _ => self.multiline_display().join("\n"),
        }
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.broadcaster.as_ref(), self.receiver.as_ref()]
    }
}
