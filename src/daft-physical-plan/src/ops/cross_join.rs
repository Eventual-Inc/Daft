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

    /// the side that is used for the outer loop is relevant for maintaining the clustering spec
    pub outer_loop_side: JoinSide,
    pub clustering_spec: ClusteringSpecRef,
}

impl CrossJoin {
    pub(crate) fn new(left: PhysicalPlanRef, right: PhysicalPlanRef) -> Self {
        let left_spec = left.clustering_spec();
        let right_spec = right.clustering_spec();

        let num_partitions = left_spec.num_partitions() * right_spec.num_partitions();

        fn try_clustering_spec_from(
            spec: &ClusteringSpec,
            num_partitions: usize,
        ) -> Option<ClusteringSpec> {
            if spec.num_partitions() == 1 {
                match spec {
                    ClusteringSpec::Hash(HashClusteringConfig { by, .. }) => Some(
                        ClusteringSpec::Hash(HashClusteringConfig::new(num_partitions, by.clone())),
                    ),
                    ClusteringSpec::Range(RangeClusteringConfig { by, descending, .. }) => {
                        Some(ClusteringSpec::Range(RangeClusteringConfig::new(
                            num_partitions,
                            by.clone(),
                            descending.clone(),
                        )))
                    }
                    _ => None,
                }
            } else {
                None
            }
        }

        let (outer_loop_side, clustering_spec) =
            if let Some(spec) = try_clustering_spec_from(&left_spec, num_partitions) {
                (JoinSide::Left, spec)
            } else if let Some(spec) = try_clustering_spec_from(&right_spec, num_partitions) {
                (JoinSide::Right, spec)
            } else {
                (
                    JoinSide::Left,
                    ClusteringSpec::Unknown(UnknownClusteringConfig::new(num_partitions)),
                )
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
