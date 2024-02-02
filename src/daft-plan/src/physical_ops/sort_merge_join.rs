use std::sync::Arc;

use daft_dsl::Expr;

use crate::{physical_plan::PhysicalPlan, JoinType};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SortMergeJoin {
    // Upstream node.
    pub left: Arc<PhysicalPlan>,
    pub right: Arc<PhysicalPlan>,
    pub left_on: Vec<Expr>,
    pub right_on: Vec<Expr>,
    pub join_type: JoinType,
    pub num_partitions: usize,
    pub left_is_larger: bool,
    pub needs_presort: bool,
}

impl SortMergeJoin {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        left: Arc<PhysicalPlan>,
        right: Arc<PhysicalPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
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
                self.left_on
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        } else {
            if !self.left_on.is_empty() {
                res.push(format!(
                    "Left on = {}",
                    self.left_on
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            if !self.right_on.is_empty() {
                res.push(format!(
                    "Right on = {}",
                    self.right_on
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
        res.push(format!("Num partitions = {}", self.num_partitions));
        res.push(format!("Left is larger = {}", self.left_is_larger));
        res.push(format!("Needs presort = {}", self.needs_presort));
        res
    }
}
