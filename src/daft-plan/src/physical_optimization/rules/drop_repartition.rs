use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

use crate::{
    physical_ops::FanoutByHash, physical_optimization::optimizer::PhysicalOptimizerRule,
    ClusteringSpec, PhysicalPlan, PhysicalPlanRef,
};

pub struct DropRepartitionPhysical {}

// if we are repartitioning but the child already has the correct spec, then don't repartition
impl PhysicalOptimizerRule for DropRepartitionPhysical {
    fn rewrite(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>> {
        plan.transform_up(|c| {
            if c.children().len() != 1 {
                return Ok(Transformed::no(c));
            }
            let children = c.children();
            let child = children.first().unwrap();
            let cur_spec = child.clustering_spec();
            if !matches!(cur_spec.as_ref(), ClusteringSpec::Hash(..)) {
                return Ok(Transformed::no(c));
            }

            match c.as_ref() {
                PhysicalPlan::FanoutByHash(FanoutByHash {
                    partition_by,
                    num_partitions,
                    ..
                }) => {
                    if *partition_by == cur_spec.partition_by()
                        && *num_partitions == cur_spec.num_partitions()
                    {
                        Ok(Transformed::yes(child.clone()))
                    } else {
                        Ok(Transformed::no(c))
                    }
                }
                // remove extra reducemerge
                // should this be its own rule?
                PhysicalPlan::ReduceMerge(..) => match child.as_ref() {
                    PhysicalPlan::FanoutByHash(..)
                    | PhysicalPlan::FanoutByRange(..)
                    | PhysicalPlan::FanoutRandom(..) => Ok(Transformed::no(c)),
                    _ => Ok(Transformed::yes(child.clone())),
                },
                _ => Ok(Transformed::no(c)),
            }
        })
    }
}
