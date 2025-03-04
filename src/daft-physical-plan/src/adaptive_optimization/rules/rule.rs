use common_error::DaftResult;
use common_treenode::{Transformed, TransformedResult};

use crate::{adaptive_optimization::optimizer::AdaptiveOptimizerConfig, PhysicalPlanRef};

pub trait AdaptiveOptimizerRule {
    fn rewrite(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>>;
}

pub enum AdaptiveRuleExecutionStrategy {
    // Apply the batch of rules only once.
    Once,
    // Apply the batch of rules multiple times, to a fixed-point or until the max
    // passes is hit.
    // If parametrized by Some(n), the batch of rules will be run a maximum
    // of n passes; if None, the number of passes is capped by the max passes argument.
    #[allow(dead_code)]
    FixedPoint(Option<usize>),
}

pub struct AdaptiveOptimizerRuleBatch {
    rules: Vec<Box<dyn AdaptiveOptimizerRule>>,
    strategy: AdaptiveRuleExecutionStrategy,
}

// A batch of AdaptiveOptimizerRules, which are run in order until
// the condition specified by the AdaptiveRuleExecutionStrategy is satisfied.
impl AdaptiveOptimizerRuleBatch {
    pub fn new(
        rules: Vec<Box<dyn AdaptiveOptimizerRule>>,
        strategy: AdaptiveRuleExecutionStrategy,
    ) -> Self {
        Self { rules, strategy }
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
        config: &AdaptiveOptimizerConfig,
    ) -> DaftResult<PhysicalPlanRef> {
        match self.strategy {
            AdaptiveRuleExecutionStrategy::Once => self.optimize_once(plan).data(),
            AdaptiveRuleExecutionStrategy::FixedPoint(passes) => {
                let passes =
                    passes.map_or(config.max_passes, |x| std::cmp::min(config.max_passes, x));
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
