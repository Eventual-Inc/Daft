use common_error::DaftResult;
use common_treenode::{Transformed, TransformedResult};

use crate::PhysicalPlanRef;

pub trait PhysicalOptimizerRule {
    fn rewrite(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>>;
}

pub enum PhysicalRuleExecutionStrategy {
    // Apply the batch of rules only once.
    Once,
    // Apply the batch of rules multiple times, to a fixed-point or until the max
    // passes is hit.
    // If parametrized by Some(n), the batch of rules will be run a maximum
    // of n passes; if None, the number of passes is capped by the max passes argument.
    #[allow(dead_code)]
    FixedPoint(Option<usize>),
}

pub struct PhysicalOptimizerRuleBatch {
    rules: Vec<Box<dyn PhysicalOptimizerRule>>,
    strategy: PhysicalRuleExecutionStrategy,
}

impl PhysicalOptimizerRuleBatch {
    pub fn new(
        rules: Vec<Box<dyn PhysicalOptimizerRule>>,
        strategy: PhysicalRuleExecutionStrategy,
    ) -> Self {
        PhysicalOptimizerRuleBatch { rules, strategy }
    }

    fn optimize_once(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>> {
        self.rules
            .iter()
            .try_fold(Transformed::no(plan), |plan, rule| {
                plan.transform_data(|p| rule.rewrite(p))
            })
    }

    pub fn optimize(
        &self,
        plan: PhysicalPlanRef,
        max_passes: usize,
    ) -> DaftResult<PhysicalPlanRef> {
        match self.strategy {
            PhysicalRuleExecutionStrategy::Once => self.optimize_once(plan).data(),
            PhysicalRuleExecutionStrategy::FixedPoint(passes) => {
                let passes = passes.map_or(max_passes, |x| std::cmp::min(max_passes, x));
                let mut plan = plan;
                for _ in 0..passes {
                    let transformed_plan = self.optimize_once(plan.clone())?;
                    if !transformed_plan.transformed {
                        break;
                    }
                    plan = transformed_plan.data;
                }
                Ok(plan)
            }
        }
    }
}
