use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

use crate::{
    physical_ops::FanoutByHash, physical_optimization::rules::PhysicalOptimizerRule,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use common_treenode::{TreeNode, TreeNodeRecursion};
    use daft_core::{
        datatypes::Field,
        schema::{Schema, SchemaRef},
    };
    use daft_dsl::{col, ExprRef};

    use crate::{
        partitioning::UnknownClusteringConfig,
        physical_ops::{EmptyScan, FanoutByHash, ReduceMerge},
        physical_optimization::rules::PhysicalOptimizerRule,
        ClusteringSpec, PhysicalPlan, PhysicalPlanRef,
    };

    use super::DropRepartitionPhysical;

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

    fn count_repartitions(plan: PhysicalPlanRef) -> usize {
        let mut total: usize = 0;
        plan.apply(|p| {
            match p.as_ref() {
                PhysicalPlan::FanoutByHash(..) => {
                    total += 1;
                }
                _ => {}
            };
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        total
    }

    // makes sure trivial repartitions are removed
    #[test]
    fn test_repartition_removed() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![
                Field::new("a", daft_core::DataType::Int32),
                Field::new("b", daft_core::DataType::Int32),
                Field::new("c", daft_core::DataType::Int32),
            ])?),
            1,
        );
        let plan = add_repartition(plan, 1, vec![col("a"), col("b")]);
        let plan = add_repartition(plan, 1, vec![col("a"), col("b")]);
        let rule = DropRepartitionPhysical {};
        let res = rule.rewrite(plan)?;
        assert!(res.transformed);
        assert_eq!(count_repartitions(res.data), 1);
        Ok(())
    }

    // makes sure different repartitions are not removed
    #[test]
    fn test_repartition_not_removed() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![
                Field::new("a", daft_core::DataType::Int32),
                Field::new("b", daft_core::DataType::Int32),
                Field::new("c", daft_core::DataType::Int32),
            ])?),
            1,
        );
        let plan = add_repartition(plan, 1, vec![col("a"), col("b")]);
        let plan = add_repartition(plan, 1, vec![col("a"), col("c")]);
        let plan = add_repartition(plan, 1, vec![col("a"), col("c"), col("b")]);
        let plan = add_repartition(plan, 1, vec![col("a")]);
        let rule = DropRepartitionPhysical {};
        let res = rule.rewrite(plan)?;
        assert!(!res.transformed);
        assert_eq!(count_repartitions(res.data), 4);
        Ok(())
    }
}
