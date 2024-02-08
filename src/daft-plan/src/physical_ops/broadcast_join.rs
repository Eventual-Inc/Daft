use daft_dsl::Expr;
use itertools::Itertools;

use crate::{physical_plan::PhysicalPlanRef, JoinType};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BroadcastJoin {
    // Upstream node.
    pub broadcaster: PhysicalPlanRef,
    pub receiver: PhysicalPlanRef,
    pub left_on: Vec<Expr>,
    pub right_on: Vec<Expr>,
    pub join_type: JoinType,
    pub is_swapped: bool,
}

impl BroadcastJoin {
    pub(crate) fn new(
        broadcaster: PhysicalPlanRef,
        receiver: PhysicalPlanRef,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        join_type: JoinType,
        is_swapped: bool,
    ) -> Self {
        Self {
            broadcaster,
            receiver,
            left_on,
            right_on,
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
        res.push(format!("Is swapped = {}", self.is_swapped));
        res
    }
}
