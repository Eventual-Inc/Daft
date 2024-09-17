use common_error::DaftResult;
use common_treenode::{DynTreeNode, Transformed, TreeNode};

use crate::{
    physical_ops::FanoutByHash, physical_optimization::rules::PhysicalOptimizerRule,
    ClusteringSpec, PhysicalPlan, PhysicalPlanRef,
};
pub struct DropRepartitionPhysical {}

// if we are repartitioning but the child already has the correct spec, then don't repartition
impl PhysicalOptimizerRule for DropRepartitionPhysical {
    fn rewrite(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>> {
        plan.transform_up(|c| {
            let children = c.arc_children();
            if children.len() != 1 {
                return Ok(Transformed::no(c));
            }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_dsl::{col, ExprRef};

    use super::DropRepartitionPhysical;
    use crate::{
        partitioning::UnknownClusteringConfig,
        physical_ops::{EmptyScan, FanoutByHash, ReduceMerge},
        physical_optimization::rules::PhysicalOptimizerRule,
        ClusteringSpec, PhysicalPlan, PhysicalPlanRef,
    };

    fn create_dummy_plan(schema: SchemaRef, num_partitions: usize) -> PhysicalPlanRef {
        PhysicalPlan::EmptyScan(EmptyScan::new(
            schema,
            ClusteringSpec::Unknown(UnknownClusteringConfig::new(num_partitions)).into(),
        ))
        .into()
    }

    fn add_repartition(
        plan: PhysicalPlanRef,
        num_partitions: usize,
        partition_by: Vec<ExprRef>,
    ) -> PhysicalPlanRef {
        PhysicalPlan::ReduceMerge(ReduceMerge::new(
            PhysicalPlan::FanoutByHash(FanoutByHash::new(plan, num_partitions, partition_by))
                .into(),
        ))
        .into()
    }

    // makes sure trivial repartitions are removed
    #[test]
    fn test_repartition_removed() -> DaftResult<()> {
        let base = create_dummy_plan(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32),
                Field::new("b", DataType::Int32),
                Field::new("c", DataType::Int32),
            ])?),
            1,
        );
        let plan = add_repartition(base.clone(), 1, vec![col("a"), col("b")]);
        let plan = add_repartition(plan, 1, vec![col("a"), col("b")]);
        let rule = DropRepartitionPhysical {};
        let res = rule.rewrite(plan)?;
        assert!(res.transformed);

        let expected_plan = add_repartition(base, 1, vec![col("a"), col("b")]);
        assert_eq!(res.data, expected_plan);
        Ok(())
    }

    // makes sure different repartitions are not removed
    #[test]
    fn test_repartition_not_removed() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32),
                Field::new("b", DataType::Int32),
                Field::new("c", DataType::Int32),
            ])?),
            1,
        );
        let plan = add_repartition(plan, 1, vec![col("a"), col("b")]);
        let plan = add_repartition(plan, 1, vec![col("a"), col("c")]);
        let plan = add_repartition(plan, 1, vec![col("a"), col("c"), col("b")]);
        let plan = add_repartition(plan, 1, vec![col("a")]);
        let rule = DropRepartitionPhysical {};
        let res = rule.rewrite(plan.clone())?;
        assert!(!res.transformed);
        assert_eq!(res.data, plan);
        Ok(())
    }
}
