use common_error::DaftResult;
use common_treenode::Transformed;

use crate::PhysicalPlanRef;

use super::rules::test_rule::TestRule;

pub trait PhysicalOptimizerRule {
    fn rewrite(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>>;
}

pub struct PhysicalOptimizer {
    rules: Vec<Box<dyn PhysicalOptimizerRule>>,
}

impl PhysicalOptimizer {
    pub fn new() -> Self {
        PhysicalOptimizer {
            rules: vec![Box::new(TestRule {})],
        }
    }

    pub fn optimize(&self, mut plan: PhysicalPlanRef) -> DaftResult<PhysicalPlanRef> {
        for rule in self.rules.iter() {
            plan = rule.rewrite(plan)?.data;
        }
        Ok(plan)
    }
}
