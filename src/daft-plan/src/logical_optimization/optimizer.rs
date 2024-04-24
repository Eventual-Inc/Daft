use std::{collections::HashSet, ops::ControlFlow, sync::Arc};

use common_error::DaftResult;

use crate::LogicalPlan;

use super::{
    logical_plan_tracker::LogicalPlanTracker,
    rules::{
        ApplyOrder, DropRepartition, OptimizerRule, PushDownFilter, PushDownLimit,
        PushDownProjection, Transformed,
    },
};

/// Config for optimizer.
#[derive(Debug)]
pub struct OptimizerConfig {
    // Default maximum number of optimization passes the optimizer will make over a fixed-point RuleBatch.
    pub default_max_optimizer_passes: usize,
}

impl OptimizerConfig {
    fn new(max_optimizer_passes: usize) -> Self {
        OptimizerConfig {
            default_max_optimizer_passes: max_optimizer_passes,
        }
    }
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        // Default to a max of 5 optimizer passes for a given batch.
        OptimizerConfig::new(5)
    }
}

pub trait OptimizerRuleInBatch: OptimizerRule + std::fmt::Debug {}

impl<T: OptimizerRule + std::fmt::Debug> OptimizerRuleInBatch for T {}

/// A batch of logical optimization rules.
#[derive(Debug)]
pub struct RuleBatch {
    // Optimization rules in this batch.
    pub rules: Vec<Box<dyn OptimizerRuleInBatch>>,
    // The rule execution strategy (once, fixed-point).
    pub strategy: RuleExecutionStrategy,
    // The application order for the entire rule batch, derived from the application
    // order of the contained rules.
    // If all rules in the batch have the same application order (e.g. top-down), the
    // optimizer will apply the rules on a single tree traversal, where a given node
    // in the plan tree will be transformed sequentially by each rule in the batch before
    // moving on to the next node.
    pub order: Option<ApplyOrder>,
}

impl RuleBatch {
    pub fn new(rules: Vec<Box<dyn OptimizerRuleInBatch>>, strategy: RuleExecutionStrategy) -> Self {
        // Get all unique application orders for the rules.
        let unique_application_orders: Vec<ApplyOrder> = rules
            .iter()
            .map(|rule| rule.apply_order())
            .collect::<HashSet<ApplyOrder>>()
            .into_iter()
            .collect();
        let order = match unique_application_orders.as_slice() {
            // All rules have the same application order, so use that as the application order for
            // the entire batch.
            [order] => Some(order.clone()),
            // If rules have different application orders, run each rule as a separate tree pass with its own application order.
            _ => None,
        };
        Self {
            rules,
            strategy,
            order,
        }
    }

    #[allow(dead_code)]
    pub fn with_order(
        rules: Vec<Box<dyn OptimizerRuleInBatch>>,
        strategy: RuleExecutionStrategy,
        order: Option<ApplyOrder>,
    ) -> Self {
        debug_assert!(order.clone().map_or(true, |order| rules
            .iter()
            .all(|rule| rule.apply_order() == order)));
        Self {
            rules,
            strategy,
            order,
        }
    }

    /// Get the maximum number of passes the optimizer should make over this rule batch.
    fn max_passes(&self, config: &OptimizerConfig) -> usize {
        use RuleExecutionStrategy::*;

        match self.strategy {
            Once => 1usize,
            FixedPoint(max_passes) => max_passes.unwrap_or(config.default_max_optimizer_passes),
        }
    }
}

/// The execution strategy for a batch of rules.
#[derive(Debug)]
pub enum RuleExecutionStrategy {
    // Apply the batch of rules only once.
    #[allow(dead_code)]
    Once,
    // Apply the batch of rules multiple times, to a fixed-point or until the max
    // passes is hit.
    // If parametrized by Some(n), the batch of rules will be run a maximum
    // of n passes; if None, the number of passes is capped by the default max
    // passes given in the OptimizerConfig.
    FixedPoint(Option<usize>),
}

/// Logical rule-based optimizer.
pub struct Optimizer {
    // Batches of rules for the optimizer to apply.
    pub rule_batches: Vec<RuleBatch>,
    // Config for optimizer.
    config: OptimizerConfig,
}

impl Optimizer {
    pub fn new(config: OptimizerConfig) -> Self {
        // Default rule batches.
        let rule_batches: Vec<RuleBatch> = vec![RuleBatch::new(
            vec![
                Box::new(DropRepartition::new()),
                Box::new(PushDownFilter::new()),
                Box::new(PushDownProjection::new()),
                Box::new(PushDownLimit::new()),
            ],
            // Use a fixed-point policy for the pushdown rules: PushDownProjection can produce a Filter node
            // at the current node, which would require another batch application in order to have a chance to push
            // that Filter node through upstream nodes.
            // TODO(Clark): Refine this fixed-point policy.
            RuleExecutionStrategy::FixedPoint(Some(3)),
        )];
        Self::with_rule_batches(rule_batches, config)
    }

    pub fn with_rule_batches(rule_batches: Vec<RuleBatch>, config: OptimizerConfig) -> Self {
        Self {
            rule_batches,
            config,
        }
    }

    /// Optimize the provided plan with this optimizer's set of rule batches.
    pub fn optimize<F>(
        &self,
        plan: Arc<LogicalPlan>,
        mut observer: F,
    ) -> DaftResult<Arc<LogicalPlan>>
    where
        F: FnMut(&LogicalPlan, &RuleBatch, usize, bool, bool),
    {
        let mut plan_tracker = LogicalPlanTracker::new(self.config.default_max_optimizer_passes);
        plan_tracker.add_plan(plan.as_ref());
        // Fold over rule batches, applying each rule batch to the tree sequentially.
        self.rule_batches.iter().try_fold(plan, |plan, batch| {
            self.optimize_with_rule_batch(batch, plan, &mut observer, &mut plan_tracker)
        })
    }

    // Optimize the provided plan with the provided rule batch.
    pub fn optimize_with_rule_batch<F>(
        &self,
        batch: &RuleBatch,
        plan: Arc<LogicalPlan>,
        observer: &mut F,
        plan_tracker: &mut LogicalPlanTracker,
    ) -> DaftResult<Arc<LogicalPlan>>
    where
        F: FnMut(&LogicalPlan, &RuleBatch, usize, bool, bool),
    {
        let result = (0..batch.max_passes(&self.config)).try_fold(
            plan,
            |plan, pass| -> ControlFlow<DaftResult<Arc<LogicalPlan>>, Arc<LogicalPlan>> {
                match self.optimize_with_rules(batch.rules.as_slice(), plan, &batch.order) {
                    Ok(Transformed::Yes(new_plan)) => {
                        // Plan was transformed by the rule batch.
                        if plan_tracker.add_plan(new_plan.as_ref()) {
                            // Transformed plan has not yet been seen by this optimizer, which means we have
                            // not reached a fixed-point or a cycle. We therefore continue applying this rule batch.
                            observer(new_plan.as_ref(), batch, pass, true, false);
                            ControlFlow::Continue(new_plan)
                        } else {
                            // We've already seen this transformed plan, which means we have hit a cycle while repeatedly
                            // applying this rule batch. We therefore stop applying this rule batch.
                            observer(new_plan.as_ref(), batch, pass, true, true);
                            ControlFlow::Break(Ok(new_plan))
                        }
                    }
                    Ok(Transformed::No(plan)) => {
                        // Plan was not transformed by the rule batch, suggesting that we have reached a fixed-point.
                        // We therefore stop applying this rule batch.
                        observer(plan.as_ref(), batch, pass, false, false);
                        ControlFlow::Break(Ok(plan))
                    }
                    // We've encountered an error, stop applying this rule batch.
                    Err(e) => ControlFlow::Break(Err(e)),
                }
            },
        );
        match result {
            ControlFlow::Continue(result) => Ok(result),
            ControlFlow::Break(result) => result,
        }
    }

    /// Optimize the provided plan with the provided rules using the provided application order.
    ///
    /// If order.is_some(), all rules are expected to have that application order.
    pub fn optimize_with_rules(
        &self,
        rules: &[Box<dyn OptimizerRuleInBatch>],
        plan: Arc<LogicalPlan>,
        order: &Option<ApplyOrder>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // Double-check that all rules have the same application order as `order`, if `order` is not None.
        debug_assert!(order.clone().map_or(true, |order| rules
            .iter()
            .all(|rule| rule.apply_order() == order)));
        match order {
            // Perform a single top-down traversal and apply all rules on each node.
            Some(ApplyOrder::TopDown) => {
                // First optimize the current node, and then it's children.
                let curr_opt = self.optimize_node(rules, plan)?;
                let children_opt =
                    self.optimize_children(rules, curr_opt.unwrap().clone(), ApplyOrder::TopDown)?;
                Ok(children_opt.or(curr_opt))
            }
            // Perform a single bottom-up traversal and apply all rules on each node.
            Some(ApplyOrder::BottomUp) => {
                // First optimize the current node's children, and then the current node.
                let children_opt = self.optimize_children(rules, plan, ApplyOrder::BottomUp)?;
                let curr_opt = self.optimize_node(rules, children_opt.unwrap().clone())?;
                Ok(curr_opt.or(children_opt))
            }
            // All rules do their own internal tree traversals.
            Some(ApplyOrder::Delegated) => self.optimize_node(rules, plan),
            // Rule batch doesn't share a single application order, so we apply each rule with its own dedicated tree pass.
            None => rules
                .windows(1)
                .try_fold(Transformed::No(plan), |plan, rule| {
                    self.optimize_with_rules(
                        rule,
                        plan.unwrap().clone(),
                        &Some(rule[0].apply_order()),
                    )
                }),
        }
    }

    /// Optimize a single plan node with the provided rules.
    ///
    /// This method does not drive traversal of the tree unless the tree is traversed by the rule itself,
    /// in rule.try_optimize().
    fn optimize_node(
        &self,
        rules: &[Box<dyn OptimizerRuleInBatch>],
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // Fold over the rules, applying each rule to this plan node sequentially.
        rules.iter().try_fold(Transformed::No(plan), |plan, rule| {
            Ok(rule.try_optimize(plan.unwrap().clone())?.or(plan))
        })
    }

    /// Optimize the children of the provided plan, updating the provided plan's pointed-to children
    /// if the children are transformed.
    fn optimize_children(
        &self,
        rules: &[Box<dyn OptimizerRuleInBatch>],
        plan: Arc<LogicalPlan>,
        order: ApplyOrder,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // Run optimization rules on children.
        let children = plan.children();
        let result = children
            .into_iter()
            .map(|child_plan| {
                self.optimize_with_rules(rules, child_plan.clone(), &Some(order.clone()))
            })
            .collect::<DaftResult<Vec<_>>>()?;
        // If the optimization rule didn't change any of the children, return without modifying the plan.
        if result.is_empty() || result.iter().all(|o| o.is_no()) {
            return Ok(Transformed::No(plan));
        }
        // Otherwise, update the parent to point to its optimized children.
        let new_children = result
            .into_iter()
            .map(|maybe_opt_child| maybe_opt_child.unwrap().clone())
            .collect::<Vec<_>>();

        // Return new plan with optimized children.
        Ok(Transformed::Yes(
            plan.with_new_children(&new_children).into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::{col, lit};

    use crate::{
        logical_ops::{Filter, Project},
        logical_optimization::rules::{ApplyOrder, OptimizerRule, Transformed},
        test::{dummy_scan_node, dummy_scan_operator},
        LogicalPlan,
    };

    use super::{Optimizer, OptimizerConfig, RuleBatch, RuleExecutionStrategy};

    /// Test that the optimizer terminates early when the plan is not transformed
    /// by a rule (i.e. a fixed-point is reached).
    #[test]
    fn early_termination_no_transform() -> DaftResult<()> {
        let optimizer = Optimizer::with_rule_batches(
            vec![RuleBatch::new(
                vec![Box::new(NoOp::new())],
                RuleExecutionStrategy::Once,
            )],
            OptimizerConfig::new(5),
        );
        let plan: Arc<LogicalPlan> =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)])).build();
        let mut pass_count = 0;
        let mut did_transform = false;
        optimizer.optimize(plan.clone(), |new_plan, _, _, transformed, _| {
            assert_eq!(new_plan, plan.as_ref());
            pass_count += 1;
            did_transform |= transformed;
        })?;
        assert!(!did_transform);
        assert_eq!(pass_count, 1);
        Ok(())
    }

    #[derive(Debug)]
    struct NoOp {}

    impl NoOp {
        pub fn new() -> Self {
            Self {}
        }
    }

    impl OptimizerRule for NoOp {
        fn apply_order(&self) -> ApplyOrder {
            ApplyOrder::TopDown
        }

        fn try_optimize(
            &self,
            plan: Arc<LogicalPlan>,
        ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
            Ok(Transformed::No(plan))
        }
    }

    /// Tests that the optimizer terminates early when a cycle is detected.
    ///
    /// This test creates a Projection -> Source plan where the projection has [1, 2, 3] projections;
    /// the optimizer will rotate the projection expressions to the left on each pass.
    ///   [1, 2, 3] -> [2, 3, 1] -> [3, 1, 2] -> [1, 2, 3]
    /// The optimization loop should therefore terminate on the 3rd pass.
    #[test]
    fn early_termination_equal_plan_cycle() -> DaftResult<()> {
        let optimizer = Optimizer::with_rule_batches(
            vec![RuleBatch::new(
                vec![Box::new(RotateProjection::new(false))],
                RuleExecutionStrategy::FixedPoint(Some(20)),
            )],
            OptimizerConfig::new(20),
        );
        let proj_exprs = vec![
            col("a").add(lit(1)),
            col("a").add(lit(2)).alias("b"),
            col("a").add(lit(3)).alias("c"),
        ];
        let plan = dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]))
            .select(proj_exprs)?
            .build();
        let mut pass_count = 0;
        let mut did_transform = false;
        optimizer.optimize(plan, |_, _, _, transformed, _| {
            pass_count += 1;
            did_transform |= transformed;
        })?;
        assert!(did_transform);
        assert_eq!(pass_count, 3);
        Ok(())
    }

    /// Tests that the optimizer terminates early when a cycle is detected.
    ///
    /// This test creates a Projection -> Source plan where the projection has [1, 2, 3] projections;
    /// the optimizer will reverse the projection expressions on the first pass and rotate them to the
    /// left on each pass thereafter.
    ///   [1, 2, 3] -> [3, 2, 1] -> [2, 1, 3] -> [1, 3, 2] -> [3, 2, 1]
    /// The optimization loop should therefore terminate on the 4th pass.
    #[test]
    fn early_termination_equal_plan_cycle_after_first() -> DaftResult<()> {
        let optimizer = Optimizer::with_rule_batches(
            vec![RuleBatch::new(
                vec![Box::new(RotateProjection::new(true))],
                RuleExecutionStrategy::FixedPoint(Some(20)),
            )],
            OptimizerConfig::new(20),
        );
        let proj_exprs = vec![
            col("a").add(lit(1)),
            col("a").add(lit(2)).alias("b"),
            col("a").add(lit(3)).alias("c"),
        ];
        let plan = dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]))
            .select(proj_exprs)?
            .build();
        let mut pass_count = 0;
        let mut did_transform = false;
        optimizer.optimize(plan, |_, _, _, transformed, _| {
            pass_count += 1;
            did_transform |= transformed;
        })?;
        assert!(did_transform);
        assert_eq!(pass_count, 4);
        Ok(())
    }

    /// Tests that the optimizer applies multiple rule batches.
    ///
    /// This test creates a Filter -> Projection -> Source plan and has 3 rule batches:
    /// (1) ([NoOp, RotateProjection], FixedPoint(20)), which should terminate due to a plan cycle.
    /// (2) ([FilterOrFalse, RotateProjection], FixedPoint(2)), which should run twice.
    /// (3) ([FilterAndTrue], Once), which should run once.
    /// The Projection has exprs [1, 2, 3], meaning that (1) should run 3 times before the cycle is hit.
    #[test]
    fn multiple_rule_batches() -> DaftResult<()> {
        let optimizer = Optimizer::with_rule_batches(
            vec![
                RuleBatch::new(
                    vec![
                        Box::new(NoOp::new()),
                        Box::new(RotateProjection::new(false)),
                    ],
                    RuleExecutionStrategy::FixedPoint(Some(20)),
                ),
                RuleBatch::new(
                    vec![
                        Box::new(FilterOrFalse::new()),
                        Box::new(RotateProjection::new(false)),
                    ],
                    RuleExecutionStrategy::FixedPoint(Some(2)),
                ),
                RuleBatch::new(
                    vec![Box::new(FilterAndTrue::new())],
                    RuleExecutionStrategy::Once,
                ),
            ],
            OptimizerConfig::new(20),
        );
        let proj_exprs = vec![
            col("a").add(lit(1)),
            col("a").add(lit(2)).alias("b"),
            col("a").add(lit(3)).alias("c"),
        ];
        let filter_predicate = col("a").lt(lit(2));
        let scan_op = dummy_scan_operator(vec![Field::new("a", DataType::Int64)]);
        let plan = dummy_scan_node(scan_op.clone())
            .select(proj_exprs.clone())?
            .filter(filter_predicate.clone())?
            .build();
        let mut pass_count = 0;
        let mut did_transform = false;
        let opt_plan = optimizer.optimize(plan, |_, _, _, transformed, _| {
            pass_count += 1;
            did_transform |= transformed;
        })?;
        assert!(did_transform);
        // 3 + 2 + 1 = 6
        assert_eq!(pass_count, 6);

        let mut new_proj_exprs = proj_exprs.clone();
        new_proj_exprs.rotate_left(2);
        let new_pred = filter_predicate
            .or(lit(false))
            .or(lit(false))
            .and(lit(true));
        let expected = dummy_scan_node(scan_op)
            .select(new_proj_exprs)?
            .filter(new_pred)?
            .build();
        assert_eq!(
            opt_plan,
            expected,
            "\n\nOptimized plan not equal to expected.\n\nOptimized:\n{}\n\nExpected:\n{}",
            opt_plan.repr_ascii(false),
            expected.repr_ascii(false)
        );
        Ok(())
    }

    #[derive(Debug)]
    struct FilterOrFalse {}

    impl FilterOrFalse {
        pub fn new() -> Self {
            Self {}
        }
    }

    impl OptimizerRule for FilterOrFalse {
        fn apply_order(&self) -> ApplyOrder {
            ApplyOrder::TopDown
        }

        fn try_optimize(
            &self,
            plan: Arc<LogicalPlan>,
        ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
            let filter = match plan.as_ref() {
                LogicalPlan::Filter(filter) => filter.clone(),
                _ => return Ok(Transformed::No(plan)),
            };
            let new_predicate = filter.predicate.or(lit(false));
            Ok(Transformed::Yes(
                LogicalPlan::from(Filter::try_new(filter.input.clone(), new_predicate)?).into(),
            ))
        }
    }

    #[derive(Debug)]
    struct FilterAndTrue {}

    impl FilterAndTrue {
        pub fn new() -> Self {
            Self {}
        }
    }

    impl OptimizerRule for FilterAndTrue {
        fn apply_order(&self) -> ApplyOrder {
            ApplyOrder::TopDown
        }

        fn try_optimize(
            &self,
            plan: Arc<LogicalPlan>,
        ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
            let filter = match plan.as_ref() {
                LogicalPlan::Filter(filter) => filter.clone(),
                _ => return Ok(Transformed::No(plan)),
            };
            let new_predicate = filter.predicate.and(lit(true));
            Ok(Transformed::Yes(
                LogicalPlan::from(Filter::try_new(filter.input.clone(), new_predicate)?).into(),
            ))
        }
    }

    #[derive(Debug)]
    struct RotateProjection {
        reverse_first: Mutex<bool>,
    }

    impl RotateProjection {
        pub fn new(reverse_first: bool) -> Self {
            Self {
                reverse_first: Mutex::new(reverse_first),
            }
        }
    }

    impl OptimizerRule for RotateProjection {
        fn apply_order(&self) -> ApplyOrder {
            ApplyOrder::TopDown
        }

        fn try_optimize(
            &self,
            plan: Arc<LogicalPlan>,
        ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
            let project = match plan.as_ref() {
                LogicalPlan::Project(project) => project.clone(),
                _ => return Ok(Transformed::No(plan)),
            };
            let mut exprs = project.projection.clone();
            let mut reverse = self.reverse_first.lock().unwrap();
            if *reverse {
                exprs.reverse();
                *reverse = false;
            } else {
                exprs.rotate_left(1);
            }
            Ok(Transformed::Yes(
                LogicalPlan::from(Project::try_new(
                    project.input.clone(),
                    exprs,
                    project.resource_request.clone(),
                )?)
                .into(),
            ))
        }
    }
}
