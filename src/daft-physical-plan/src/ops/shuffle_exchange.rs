use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use daft_dsl::ExprRef;
use daft_logical_plan::partitioning::{
    ClusteringSpec, HashClusteringConfig, RandomClusteringConfig, RangeClusteringConfig,
    UnknownClusteringConfig,
};
use serde::{Deserialize, Serialize};

use crate::{impl_default_tree_display, PhysicalPlanRef};

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
            ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge { target_spec, .. } => {
                target_spec.clone()
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShuffleExchangeStrategy {
    /// Fully materialize the data after the Map, and then pull results from the Reduce.
    NaiveFullyMaterializingMapReduce { target_spec: Arc<ClusteringSpec> },

    /// Sequentially splits/coalesce partitions in order to meet a target number of partitions
    SplitOrCoalesceToTargetNum { target_num_partitions: usize },

    MapReduceWithPreShuffleMerge {
        pre_shuffle_merge_threshold: usize,
        target_spec: Arc<ClusteringSpec>,
    },
}

impl ShuffleExchange {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("ShuffleExchange:".to_string());
        match &self.strategy {
            ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce { target_spec } => {
                res.push("Strategy: NaiveFullyMaterializingMapReduce".to_string());
                res.push(format!("Target Spec: {:?}", target_spec));
                res.push(format!(
                    "Number of Partitions: {} → {}",
                    self.input.clustering_spec().num_partitions(),
                    target_spec.num_partitions(),
                ));
            }
            ShuffleExchangeStrategy::SplitOrCoalesceToTargetNum {
                target_num_partitions,
            } => {
                let input_num_partitions = self.input.clustering_spec().num_partitions();
                res.push("Strategy: SplitOrCoalesceToTargetNum".to_string());
                res.push(format!(
                    "{} Partitions: {} → {}",
                    if input_num_partitions >= *target_num_partitions {
                        "Coalescing"
                    } else {
                        "Splitting"
                    },
                    input_num_partitions,
                    target_num_partitions,
                ));
            }
            ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge { target_spec, .. } => {
                res.push("Strategy: MapReduceWithPreShuffleMerge".to_string());
                res.push(format!("Target Spec: {:?}", target_spec));
                res.push(format!(
                    "Number of Partitions: {} → {}",
                    self.input.clustering_spec().num_partitions(),
                    target_spec.num_partitions(),
                ));
            }
        }
        res
    }
}

impl_default_tree_display!(ShuffleExchange);

/// Factory of ShuffleExchanges
///
/// This provides an abstraction where we can select the most appropriate strategies based on various
/// heuristics such as number of partitions and the currently targeted backend's available resources.
pub struct ShuffleExchangeFactory {
    input: PhysicalPlanRef,
}

impl ShuffleExchangeFactory {
    pub fn new(input: PhysicalPlanRef) -> Self {
        Self { input }
    }

    pub fn get_hash_partitioning(
        &self,
        by: Vec<ExprRef>,
        num_partitions: usize,
        cfg: Option<&DaftExecutionConfig>,
    ) -> ShuffleExchange {
        let clustering_spec = Arc::new(ClusteringSpec::Hash(HashClusteringConfig::new(
            num_partitions,
            by,
        )));

        let strategy = match cfg {
            Some(cfg) if cfg.shuffle_algorithm == "pre_shuffle_merge" => {
                ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge {
                    target_spec: clustering_spec,
                    pre_shuffle_merge_threshold: cfg.pre_shuffle_merge_threshold,
                }
            }
            _ => ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce {
                target_spec: clustering_spec,
            },
        };

        ShuffleExchange {
            input: self.input.clone(),
            strategy,
        }
    }

    pub fn get_range_partitioning(
        &self,
        by: Vec<ExprRef>,
        descending: Vec<bool>,
        num_partitions: usize,
        cfg: Option<&DaftExecutionConfig>,
    ) -> ShuffleExchange {
        let clustering_spec = Arc::new(ClusteringSpec::Range(RangeClusteringConfig::new(
            num_partitions,
            by,
            descending,
        )));

        let strategy = match cfg {
            Some(cfg) if cfg.shuffle_algorithm == "pre_shuffle_merge" => {
                ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge {
                    target_spec: clustering_spec,
                    pre_shuffle_merge_threshold: cfg.pre_shuffle_merge_threshold,
                }
            }
            _ => ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce {
                target_spec: clustering_spec,
            },
        };

        ShuffleExchange {
            input: self.input.clone(),
            strategy,
        }
    }

    pub fn get_random_partitioning(
        &self,
        num_partitions: usize,
        cfg: Option<&DaftExecutionConfig>,
    ) -> ShuffleExchange {
        let clustering_spec = Arc::new(ClusteringSpec::Random(RandomClusteringConfig::new(
            num_partitions,
        )));

        let strategy = match cfg {
            Some(cfg) if cfg.shuffle_algorithm == "pre_shuffle_merge" => {
                ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge {
                    target_spec: clustering_spec,
                    pre_shuffle_merge_threshold: cfg.pre_shuffle_merge_threshold,
                }
            }
            _ => ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce {
                target_spec: clustering_spec,
            },
        };

        ShuffleExchange {
            input: self.input.clone(),
            strategy,
        }
    }

    pub fn get_split_or_coalesce(&self, num_partitions: usize) -> ShuffleExchange {
        let strategy = ShuffleExchangeStrategy::SplitOrCoalesceToTargetNum {
            target_num_partitions: num_partitions,
        };
        ShuffleExchange {
            input: self.input.clone(),
            strategy,
        }
    }
}
