use std::sync::Arc;

use common_display::tree::TreeDisplay;
use daft_core::join::JoinSide;
use daft_logical_plan::{
    partitioning::{
        ClusteringSpecRef, HashClusteringConfig, RangeClusteringConfig, UnknownClusteringConfig,
    },
    ClusteringSpec,
};
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CrossJoin {
    pub left: PhysicalPlanRef,
    pub right: PhysicalPlanRef,
    pub outer_loop_side: JoinSide,
}

impl CrossJoin {
    pub(crate) fn new(
        left: PhysicalPlanRef,
        right: PhysicalPlanRef,
        outer_loop_side: JoinSide,
    ) -> Self {
        Self {
            left,
            right,
            outer_loop_side,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec!["CrossJoin".to_string()]
    }
}

impl TreeDisplay for CrossJoin {
    fn display_as(&self, _level: common_display::DisplayLevel) -> String {
        self.get_name()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }
}
