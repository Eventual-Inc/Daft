use common_display::tree::TreeDisplay;
use daft_core::join::JoinType;
use daft_dsl::ExprRef;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SortMergeJoin {
    // Upstream node.
    pub left: PhysicalPlanRef,
    pub right: PhysicalPlanRef,
    pub left_on: Vec<ExprRef>,
    pub right_on: Vec<ExprRef>,
    pub join_type: JoinType,
    pub num_partitions: usize,
    pub left_is_larger: bool,
    pub needs_presort: bool,
}

impl SortMergeJoin {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        left: PhysicalPlanRef,
        right: PhysicalPlanRef,
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        join_type: JoinType,
        num_partitions: usize,
        left_is_larger: bool,
        needs_presort: bool,
    ) -> Self {
        Self {
            left,
            right,
            left_on,
            right_on,
            join_type,
            num_partitions,
            left_is_larger,
            needs_presort,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("SortMergeJoin: Type = {}", self.join_type));
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
        res.push(format!("Num partitions = {}", self.num_partitions));
        res.push(format!("Left is larger = {}", self.left_is_larger));
        res.push(format!("Needs presort = {}", self.needs_presort));
        res
    }
}

impl TreeDisplay for SortMergeJoin {
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
