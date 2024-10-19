use std::sync::Arc;

use daft_dsl::ExprRef;
use serde::{Deserialize, Serialize};

use crate::{
    impl_default_tree_display,
    partitioning::{
        HashClusteringConfig, RandomClusteringConfig, RangeClusteringConfig,
        UnknownClusteringConfig,
    },
    ClusteringSpec, PhysicalPlanRef,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShuffleExchange {
    pub input: PhysicalPlanRef,
    pub strategy: ShuffleExchangeStrategy,
}

impl ShuffleExchange {
    /// Retrieve the output clustering spec associated with this ShuffleExchange
    pub fn clustering_spec(&self) -> Arc<ClusteringSpec> {
        match &self.strategy {
            ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce { target_spec } => {
                target_spec.clone()
            }
            ShuffleExchangeStrategy::SplitOrCoalesceToTargetNum {
                target_num_partitions,
            } => Arc::new(ClusteringSpec::Unknown(UnknownClusteringConfig::new(
                *target_num_partitions,
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShuffleExchangeStrategy {
    /// Fully materialize the data after the Map, and then pull results from the Reduce.
    NaiveFullyMaterializingMapReduce { target_spec: Arc<ClusteringSpec> },

    /// Sequentially splits/coalesce partitions in order to meet a target number of partitions
    SplitOrCoalesceToTargetNum { target_num_partitions: usize },
}

impl ShuffleExchange {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("ShuffleExchange:".to_string());
        match &self.strategy {
            ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce { target_spec } => {
                res.push("  Strategy: NaiveFullyMaterializingMapReduce".to_string());
                res.push(format!("  Target Spec: {:?}", target_spec));
            }
            ShuffleExchangeStrategy::SplitOrCoalesceToTargetNum {
                target_num_partitions,
            } => {
                res.push("  Strategy: SplitOrCoalesceToTargetNum".to_string());
                res.push(format!(
                    "  Target Num Partitions: {:?}",
                    target_num_partitions
                ));
            }
        }
        res
    }
}

impl_default_tree_display!(ShuffleExchange);

/// Builder of ShuffleExchanges
///
/// This provides an abstraction where we can select the most appropriate strategies based on various
/// heuristics such as number of partitions and the currently targeted backend's available resources.
pub struct ShuffleExchangeBuilder {
    input: PhysicalPlanRef,
    strategy: ShuffleExchangeStrategy,
}

impl ShuffleExchangeBuilder {
    pub fn new(input: PhysicalPlanRef) -> Self {
        let input_num_partitions = input.as_ref().clustering_spec().num_partitions();
        Self {
            input,
            strategy: ShuffleExchangeStrategy::SplitOrCoalesceToTargetNum {
                target_num_partitions: input_num_partitions,
            },
        }
    }

    pub fn with_hash_partitioning(mut self, by: Vec<ExprRef>, num_partitions: usize) -> Self {
        self.strategy = ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce {
            target_spec: Arc::new(ClusteringSpec::Hash(HashClusteringConfig::new(
                num_partitions,
                by,
            ))),
        };
        self
    }

    pub fn with_range_partitioning(
        mut self,
        by: Vec<ExprRef>,
        descending: Vec<bool>,
        num_partitions: usize,
    ) -> Self {
        self.strategy = ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce {
            target_spec: Arc::new(ClusteringSpec::Range(RangeClusteringConfig::new(
                num_partitions,
                by,
                descending,
            ))),
        };
        self
    }

    pub fn with_random_partitioning(mut self, num_partitions: usize) -> Self {
        self.strategy = ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce {
            target_spec: Arc::new(ClusteringSpec::Random(RandomClusteringConfig::new(
                num_partitions,
            ))),
        };
        self
    }

    pub fn with_split_or_coalesce(mut self, num_partitions: usize) -> Self {
        self.strategy = ShuffleExchangeStrategy::SplitOrCoalesceToTargetNum {
            target_num_partitions: num_partitions,
        };
        self
    }

    pub fn build(self) -> ShuffleExchange {
        ShuffleExchange {
            input: self.input,
            strategy: self.strategy,
        }
    }
}
