use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use daft_dsl::ExprRef;
use daft_io::{SourceType, parse_url};
use daft_logical_plan::partitioning::{
    ClusteringSpec, HashClusteringConfig, RandomClusteringConfig, RangeClusteringConfig,
    UnknownClusteringConfig,
};
use serde::{Deserialize, Serialize};

use crate::{PhysicalPlanRef, impl_default_tree_display};

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
            ShuffleExchangeStrategy::FlightShuffle { target_spec, .. } => target_spec.clone(),
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

    FlightShuffle {
        target_spec: Arc<ClusteringSpec>,
        shuffle_dirs: Vec<String>,
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
            ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge {
                target_spec,
                pre_shuffle_merge_threshold,
            } => {
                res.push("Strategy: MapReduceWithPreShuffleMerge".to_string());
                res.push(format!("Target Spec: {:?}", target_spec));
                res.push(format!(
                    "Number of Partitions: {} → {}",
                    self.input.clustering_spec().num_partitions(),
                    target_spec.num_partitions(),
                ));
                res.push(format!(
                    "Pre-Shuffle Merge Threshold: {}",
                    pre_shuffle_merge_threshold
                ));
            }
            ShuffleExchangeStrategy::FlightShuffle {
                target_spec,
                shuffle_dirs,
            } => {
                res.push("Strategy: FlightShuffle".to_string());
                res.push(format!("Target Spec: {:?}", target_spec));
                res.push(format!("Shuffle Dirs: {}", shuffle_dirs.join(", ")));
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
    const PARTITION_THRESHOLD_TO_USE_PRE_SHUFFLE_MERGE: usize = 200;

    pub fn new(input: PhysicalPlanRef) -> Self {
        Self { input }
    }

    fn should_use_pre_shuffle_merge(
        &self,
        input_num_partitions: usize,
        target_num_partitions: usize,
    ) -> bool {
        match input_num_partitions.checked_mul(target_num_partitions) {
            Some(total) => {
                let geometric_mean = (total as f64).sqrt() as usize;
                geometric_mean > Self::PARTITION_THRESHOLD_TO_USE_PRE_SHUFFLE_MERGE
            }
            None => {
                log::warn!(
                    "Partition count multiplication overflow: {} * {}, using pre-shuffle merge",
                    input_num_partitions,
                    target_num_partitions
                );
                true // Overflow means definitely should use merge
            }
        }
    }

    fn get_shuffle_strategy(
        &self,
        clustering_spec: Arc<ClusteringSpec>,
        cfg: Option<&DaftExecutionConfig>,
    ) -> DaftResult<ShuffleExchangeStrategy> {
        let strategy = match cfg {
            Some(cfg) if cfg.shuffle_algorithm == "pre_shuffle_merge" => {
                ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge {
                    target_spec: clustering_spec,
                    pre_shuffle_merge_threshold: cfg.pre_shuffle_merge_threshold,
                }
            }
            Some(cfg) if cfg.shuffle_algorithm == "map_reduce" => {
                ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce {
                    target_spec: clustering_spec,
                }
            }
            Some(cfg) if cfg.shuffle_algorithm == "flight_shuffle" => {
                if cfg.flight_shuffle_dirs.is_empty() {
                    return Err(DaftError::ValueError(
                        "flight_shuffle_dirs must be non-empty to use flight shuffle".to_string(),
                    ));
                }
                if cfg
                    .flight_shuffle_dirs
                    .iter()
                    .any(|dir| !matches!(parse_url(dir).unwrap().0, SourceType::File))
                {
                    return Err(DaftError::ValueError(
                        "Flight_shuffle_dirs must be valid file paths".to_string(),
                    ));
                }
                ShuffleExchangeStrategy::FlightShuffle {
                    target_spec: clustering_spec,
                    shuffle_dirs: cfg.flight_shuffle_dirs.clone(),
                }
            }
            Some(cfg) if cfg.shuffle_algorithm == "auto" => {
                if self.should_use_pre_shuffle_merge(
                    self.input.clustering_spec().num_partitions(),
                    clustering_spec.num_partitions(),
                ) {
                    ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge {
                        target_spec: clustering_spec,
                        pre_shuffle_merge_threshold: cfg.pre_shuffle_merge_threshold,
                    }
                } else {
                    ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce {
                        target_spec: clustering_spec,
                    }
                }
            }
            None => {
                if self.should_use_pre_shuffle_merge(
                    self.input.clustering_spec().num_partitions(),
                    clustering_spec.num_partitions(),
                ) {
                    ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge {
                        target_spec: clustering_spec,
                        pre_shuffle_merge_threshold: DaftExecutionConfig::default()
                            .pre_shuffle_merge_threshold,
                    }
                } else {
                    ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce {
                        target_spec: clustering_spec,
                    }
                }
            }
            _ => unreachable!(),
        };
        Ok(strategy)
    }

    pub fn get_hash_partitioning(
        &self,
        by: Vec<ExprRef>,
        num_partitions: usize,
        cfg: Option<&DaftExecutionConfig>,
    ) -> DaftResult<ShuffleExchange> {
        let clustering_spec = Arc::new(ClusteringSpec::Hash(HashClusteringConfig::new(
            num_partitions,
            by,
        )));

        let strategy = self.get_shuffle_strategy(clustering_spec, cfg)?;

        Ok(ShuffleExchange {
            input: self.input.clone(),
            strategy,
        })
    }

    pub fn get_range_partitioning(
        &self,
        by: Vec<ExprRef>,
        descending: Vec<bool>,
        num_partitions: usize,
        cfg: Option<&DaftExecutionConfig>,
    ) -> DaftResult<ShuffleExchange> {
        let clustering_spec = Arc::new(ClusteringSpec::Range(RangeClusteringConfig::new(
            num_partitions,
            by,
            descending,
        )));

        let strategy = self.get_shuffle_strategy(clustering_spec, cfg)?;

        Ok(ShuffleExchange {
            input: self.input.clone(),
            strategy,
        })
    }

    pub fn get_random_partitioning(
        &self,
        num_partitions: usize,
        cfg: Option<&DaftExecutionConfig>,
    ) -> DaftResult<ShuffleExchange> {
        let clustering_spec = Arc::new(ClusteringSpec::Random(RandomClusteringConfig::new(
            num_partitions,
        )));

        let strategy = self.get_shuffle_strategy(clustering_spec, cfg)?;

        Ok(ShuffleExchange {
            input: self.input.clone(),
            strategy,
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a dummy PhysicalPlanRef for testing
    fn create_dummy_plan() -> PhysicalPlanRef {
        use crate::InMemoryScan;
        use daft_schema::schema::Schema;
        use std::sync::Arc;

        let schema = Arc::new(Schema::empty());
        Arc::new(
            InMemoryScan::new(
                schema,
                vec![],
                Arc::new(ClusteringSpec::Unknown(UnknownClusteringConfig::new(1))),
                Default::default(),
            )
            .into(),
        )
    }

    #[test]
    fn test_should_use_pre_shuffle_merge_small_partitions() {
        let factory = ShuffleExchangeFactory::new(create_dummy_plan());
        // Small partition counts should not trigger pre-shuffle merge
        assert!(!factory.should_use_pre_shuffle_merge(10, 10));
    }

    #[test]
    fn test_should_use_pre_shuffle_merge_large_partitions() {
        let factory = ShuffleExchangeFactory::new(create_dummy_plan());
        // Large partition counts should trigger pre-shuffle merge
        assert!(factory.should_use_pre_shuffle_merge(1000, 1000));
    }

    #[test]
    fn test_should_use_pre_shuffle_merge_overflow() {
        let factory = ShuffleExchangeFactory::new(create_dummy_plan());
        // Test with partition counts that would overflow when multiplied
        // usize::MAX / 2 * usize::MAX / 2 would overflow
        let half_max = usize::MAX / 2;
        assert!(factory.should_use_pre_shuffle_merge(half_max, half_max));
    }

    #[test]
    fn test_should_use_pre_shuffle_merge_no_overflow_boundary() {
        let factory = ShuffleExchangeFactory::new(create_dummy_plan());
        // Test values near the overflow boundary
        let sqrt_max = (usize::MAX as f64).sqrt() as usize;
        // This should not overflow but should still trigger merge due to large geometric mean
        assert!(factory.should_use_pre_shuffle_merge(sqrt_max, sqrt_max));
    }

    #[test]
    fn test_should_use_pre_shuffle_merge_asymmetric() {
        let factory = ShuffleExchangeFactory::new(create_dummy_plan());
        // Test with asymmetric partition counts
        assert!(!factory.should_use_pre_shuffle_merge(1, 100));
        assert!(factory.should_use_pre_shuffle_merge(10000, 10000));
    }
}
