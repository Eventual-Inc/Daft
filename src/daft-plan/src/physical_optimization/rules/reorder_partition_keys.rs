use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{is_partition_compatible, ExprRef};

use crate::{
    physical_ops::FanoutByHash,
    physical_optimization::{optimizer::PhysicalOptimizerRule, plan_context::PlanContext},
    PhysicalPlan, PhysicalPlanRef,
};

pub struct ReorderPartitionKeys {}

type PartitionContext = PlanContext<Vec<ExprRef>>;

impl PhysicalOptimizerRule for ReorderPartitionKeys {
    fn rewrite(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>> {
        let plan_context = PartitionContext::new_default(plan);

        let res_transformed = plan_context.transform_down(|c| match c.plan.as_ref() {
            PhysicalPlan::FanoutByHash(FanoutByHash {
                input,
                partition_by,
                num_partitions,
            }) => {
                if is_partition_compatible(&c.context, partition_by) {
                    let new_plan = PhysicalPlan::FanoutByHash(FanoutByHash {
                        input: input.clone(),
                        partition_by: c.context.clone(),
                        num_partitions: *num_partitions,
                    });
                    Ok(Transformed::yes(c.with_plan(new_plan.arced()).propagate()))
                } else {
                    let new_context = partition_by.clone();
                    Ok(Transformed::no(c.with_context(new_context)))
                }
            }
            _ => Ok(Transformed::no(c.propagate())),
        })?;
        res_transformed.map_data(|c| Ok(c.plan))
    }
}
