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
    pub clustering_spec: ClusteringSpecRef,
}

impl CrossJoin {
    pub(crate) fn new(
        left: PhysicalPlanRef,
        right: PhysicalPlanRef,
        outer_loop_side: JoinSide,
    ) -> Self {
        let left_spec = left.clustering_spec();
        let right_spec = right.clustering_spec();

        let num_partitions = left_spec.num_partitions() * right_spec.num_partitions();

        let (outer_spec, inner_spec) = match outer_loop_side {
            JoinSide::Left => (left_spec, right_spec),
            JoinSide::Right => (right_spec, left_spec),
        };

        let clustering_spec = if inner_spec.num_partitions() == 1 {
            match outer_spec.as_ref() {
                ClusteringSpec::Hash(HashClusteringConfig { by, .. }) => {
                    ClusteringSpec::Hash(HashClusteringConfig::new(num_partitions, by.clone()))
                }
                ClusteringSpec::Range(RangeClusteringConfig { by, descending, .. }) => {
                    ClusteringSpec::Range(RangeClusteringConfig::new(
                        num_partitions,
                        by.clone(),
                        descending.clone(),
                    ))
                }
                _ => ClusteringSpec::Unknown(UnknownClusteringConfig::new(num_partitions)),
            }
        } else {
            ClusteringSpec::Unknown(UnknownClusteringConfig::new(num_partitions))
        };

        Self {
            left,
            right,
            outer_loop_side,
            clustering_spec: Arc::new(clustering_spec),
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
