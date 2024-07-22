use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};

use crate::{
    physical_ops::Limit, physical_optimization::optimizer::PhysicalOptimizerRule, PhysicalPlan,
    PhysicalPlanRef,
};

pub struct TestRule {}

impl PhysicalOptimizerRule for TestRule {
    fn rewrite(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>> {
        let mut cur_num = 1234;

        plan.transform_up(|p| match p.as_ref() {
            PhysicalPlan::Limit(_) => Ok(Transformed {
                data: p,
                transformed: false,
                tnr: TreeNodeRecursion::Continue,
            }),
            _ => {
                let limit = PhysicalPlan::Limit(Limit {
                    input: p,
                    limit: cur_num,
                    eager: false,
                    num_partitions: 1,
                });
                cur_num += 1;
                Ok(Transformed {
                    data: limit.arced(),
                    transformed: true,
                    tnr: TreeNodeRecursion::Continue,
                })
            }
        })
    }
}
