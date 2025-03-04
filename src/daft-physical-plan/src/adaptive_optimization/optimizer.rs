use common_error::DaftResult;

use super::rules::AdaptiveOptimizerRuleBatch;
use crate::PhysicalPlanRef;

pub struct AdaptiveOptimizerConfig {
    // The upper bound on the number of passes a rule batch can run.
    // Depending on its configuration a rule batch may run fewer passes.
    // Default is 5
    pub max_passes: usize,
}

impl AdaptiveOptimizerConfig {
    #[allow(dead_code)] // used in test
    pub fn new(max_passes: usize) -> Self {
        Self { max_passes }
    }
}

impl Default for AdaptiveOptimizerConfig {
    fn default() -> Self {
        Self { max_passes: 5 }
    }
}

pub struct AdaptiveOptimizer {
    rule_batches: Vec<AdaptiveOptimizerRuleBatch>,
    config: AdaptiveOptimizerConfig,
}

impl AdaptiveOptimizer {
    #[allow(dead_code)] // used in test
    pub fn new(
        rule_batches: Vec<AdaptiveOptimizerRuleBatch>,
        config: AdaptiveOptimizerConfig,
    ) -> Self {
        Self {
            rule_batches,
            config,
        }
    }

    pub fn optimize(&self, mut plan: PhysicalPlanRef) -> DaftResult<PhysicalPlanRef> {
        for batch in &self.rule_batches {
            plan = batch.optimize(plan, &self.config)?;
        }
        Ok(plan)
    }
}
