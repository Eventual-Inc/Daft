use std::sync::Arc;

use common_error::DaftResult;

use crate::LogicalPlan;

use super::rules::{ApplyOrder, OptimizerRule, PushDownFilter, PushDownProjection};

pub struct OptimizerConfig {
    // Maximum number of optimization passes the optimizer will make over the logical plan.
    pub max_optimizer_passes: usize,
}

impl OptimizerConfig {
    fn new(max_optimizer_passes: usize) -> Self {
        OptimizerConfig {
            max_optimizer_passes,
        }
    }
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        OptimizerConfig::new(3usize)
    }
}

/// Logical rule-based optimizer.
pub struct Optimizer {
    pub rules: Vec<Arc<dyn OptimizerRule>>,
}

impl Optimizer {
    pub fn new() -> Self {
        let rules: Vec<Arc<dyn OptimizerRule>> = vec![
            Arc::new(PushDownFilter::new()),
            Arc::new(PushDownProjection::new()),
        ];
        Self::with_rules(rules)
    }
    pub fn with_rules(rules: Vec<Arc<dyn OptimizerRule>>) -> Self {
        Self { rules }
    }

    pub fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        config: &OptimizerConfig,
    ) -> DaftResult<Arc<LogicalPlan>> {
        let mut new_plan = plan.clone();
        let mut i = 0;
        while i < config.max_optimizer_passes {
            for rule in &self.rules {
                let result = self.optimize_with_rule(rule, &new_plan);
                match result {
                    Ok(Some(plan)) => {
                        new_plan = plan;
                    }
                    Ok(None) => {}
                    // TODO(Clark): Return nice optimization error to user.
                    Err(e) => panic!("Optimization failed: {:?}", e),
                }
            }
            // TODO(Clark): Terminate optimization loop if logical plan has not changed on this optimization pass.
            i += 1;
        }
        Ok(new_plan)
    }

    pub fn optimize_with_rule(
        &self,
        rule: &Arc<dyn OptimizerRule>,
        plan: &Arc<LogicalPlan>,
    ) -> DaftResult<Option<Arc<LogicalPlan>>> {
        match rule.apply_order() {
            Some(ApplyOrder::TopDown) => {
                // First optimize the current node, and then it's children.
                let curr_opt = self.optimize_node(rule, plan)?;
                let children_opt = match &curr_opt {
                    Some(opt_plan) => self.optimize_children(rule, opt_plan)?,
                    None => self.optimize_children(rule, plan)?,
                };
                Ok(children_opt.or(curr_opt))
            }
            Some(ApplyOrder::BottomUp) => {
                // First optimize the current node's children, and then the current node.
                let children_opt = self.optimize_children(rule, plan)?;
                let curr_opt = match &children_opt {
                    Some(opt_plan) => self.optimize_node(rule, opt_plan)?,
                    None => self.optimize_node(rule, plan)?,
                };
                Ok(curr_opt.or(children_opt))
            }
            None => rule.try_optimize(plan.as_ref()),
        }
    }

    fn optimize_node(
        &self,
        rule: &Arc<dyn OptimizerRule>,
        plan: &Arc<LogicalPlan>,
    ) -> DaftResult<Option<Arc<LogicalPlan>>> {
        // TODO(Clark): Add optimization rule batching.
        rule.try_optimize(plan.as_ref())
    }

    fn optimize_children(
        &self,
        rule: &Arc<dyn OptimizerRule>,
        plan: &LogicalPlan,
    ) -> DaftResult<Option<Arc<LogicalPlan>>> {
        // Run optimization rule on children.
        let children = plan.children();
        let result = children
            .iter()
            .map(|child_plan| self.optimize_with_rule(rule, child_plan))
            .collect::<DaftResult<Vec<_>>>()?;
        // If the optimization rule didn't change any of the children, return without modifying the plan.
        if result.is_empty() || result.iter().all(|o| o.is_none()) {
            return Ok(None);
        }
        // Otherwise, update that parent to point to its optimized children.
        let new_children = result
            .into_iter()
            .zip(children.iter())
            .map(|(opt_child, old_child)| opt_child.unwrap_or_else(|| (*old_child).clone()))
            .collect::<Vec<_>>();

        Ok(Some(plan.with_new_children(&new_children)))
    }
}
