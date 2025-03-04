use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{DynTreeNode, Transformed, TreeNode};
use daft_logical_plan::partitioning::{
    ClusteringSpec, HashClusteringConfig, RandomClusteringConfig, RangeClusteringConfig,
    UnknownClusteringConfig,
};

use crate::{
    adaptive_optimization::rules::AdaptiveOptimizerRule,
    ops::{InMemoryScan, ShuffleExchange, ShuffleExchangeFactory, Sort},
    PhysicalPlan, PhysicalPlanRef,
};

pub struct AdaptRepartition {
    config: Arc<DaftExecutionConfig>,
}

impl AdaptRepartition {
    pub fn new(config: Arc<DaftExecutionConfig>) -> Self {
        Self { config }
    }
}

// if we are repartitioning but the child already has the correct spec, then don't repartition
impl AdaptiveOptimizerRule for AdaptRepartition {
    fn rewrite(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>> {
        plan.transform_up(|p| match p.as_ref() {
            PhysicalPlan::ShuffleExchange(ShuffleExchange {
                input, can_adapt, ..
            }) if *can_adapt => match input.as_ref() {
                PhysicalPlan::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
                    let clustering_spec = p.clustering_spec();
                    let size_bytes = in_memory_info.size_bytes;
                    let default_partition_size_bytes = self.config.shuffle_partition_size_bytes;
                    let num_partitions = size_bytes.div_ceil(default_partition_size_bytes);
                    if num_partitions == clustering_spec.num_partitions() {
                        return Ok(Transformed::no(p));
                    }

                    if num_partitions == 1 {
                        let coalesce = ShuffleExchangeFactory::new(input.clone())
                            .get_split_or_coalesce(num_partitions, false);
                        return Ok(Transformed::yes(
                            PhysicalPlan::ShuffleExchange(coalesce).arced(),
                        ));
                    } else {
                        let factory = ShuffleExchangeFactory::new(input.clone());
                        let shuffle = match clustering_spec.as_ref() {
                            ClusteringSpec::Hash(HashClusteringConfig { by, .. }) => factory
                                .get_hash_partitioning(
                                    by.clone(),
                                    num_partitions,
                                    Some(self.config.as_ref()),
                                    false,
                                ),
                            ClusteringSpec::Unknown(UnknownClusteringConfig { .. }) => {
                                factory.get_split_or_coalesce(num_partitions, false)
                            }
                            ClusteringSpec::Range(RangeClusteringConfig {
                                by, descending, ..
                            }) => factory.get_range_partitioning(
                                by.clone(),
                                descending.clone(),
                                num_partitions,
                                Some(self.config.as_ref()),
                                false,
                            ),
                            ClusteringSpec::Random(RandomClusteringConfig { .. }) => factory
                                .get_random_partitioning(
                                    num_partitions,
                                    Some(self.config.as_ref()),
                                    false,
                                ),
                        };
                        return Ok(Transformed::yes(
                            PhysicalPlan::ShuffleExchange(shuffle).arced(),
                        ));
                    }
                }
                _ => Ok(Transformed::no(p)),
            },
            PhysicalPlan::Sort(Sort {
                input,
                sort_by,
                descending,
                nulls_first,
                num_partitions,
                ..
            }) if num_partitions.is_none() => match input.as_ref() {
                PhysicalPlan::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
                    let size_bytes = in_memory_info.size_bytes;
                    let default_partition_size_bytes = self.config.shuffle_partition_size_bytes;
                    let num_partitions = size_bytes.div_ceil(default_partition_size_bytes);
                    Ok(Transformed::yes(
                        PhysicalPlan::Sort(Sort::new(
                            input.clone(),
                            sort_by.clone(),
                            descending.clone(),
                            nulls_first.clone(),
                            Some(num_partitions),
                        ))
                        .arced(),
                    ))
                }
                _ => Ok(Transformed::no(p)),
            },
            _ => Ok(Transformed::no(p)),
        })
    }
}
