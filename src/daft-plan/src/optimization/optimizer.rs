use std::{collections::HashSet, ops::ControlFlow, sync::Arc};

use common_error::DaftResult;

use crate::LogicalPlan;

use super::{
    logical_plan_tracker::LogicalPlanTracker,
    rules::{ApplyOrder, OptimizerRule, PushDownFilter, Transformed},
};

// Config for optimizer.
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

pub struct RuleBatch {
    pub rules: Vec<Box<dyn OptimizerRule>>,
    pub strategy: RuleExecutionStrategy,
    pub order: Option<ApplyOrder>,
}

impl RuleBatch {
    pub fn new(rules: Vec<Box<dyn OptimizerRule>>, strategy: RuleExecutionStrategy) -> Self {
        let unique_application_orders: Vec<ApplyOrder> = rules
            .iter()
            .map(|rule| rule.apply_order())
            .collect::<HashSet<ApplyOrder>>()
            .into_iter()
            .collect();
        let order = match unique_application_orders.as_slice() {
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

    fn max_passes(&self, config: &OptimizerConfig) -> usize {
        use RuleExecutionStrategy::*;

        match self.strategy {
            Once => 1usize,
            FixedPoint(max_passes) => max_passes.unwrap_or(config.default_max_optimizer_passes),
        }
    }
}

pub enum RuleExecutionStrategy {
    Once,
    #[allow(dead_code)]
    FixedPoint(Option<usize>),
}

/// Logical rule-based optimizer.
pub struct Optimizer {
    pub rule_batches: Vec<RuleBatch>,
    config: OptimizerConfig,
}

impl Optimizer {
    pub fn new(config: OptimizerConfig) -> Self {
        // Default rule batches.
        let rule_batches: Vec<RuleBatch> = vec![RuleBatch::new(
            vec![Box::new(PushDownFilter::new())],
            RuleExecutionStrategy::Once,
        )];
        Self::with_rule_batches(rule_batches, config)
    }
    pub fn with_rule_batches(rule_batches: Vec<RuleBatch>, config: OptimizerConfig) -> Self {
        Self {
            rule_batches,
            config,
        }
    }

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
        self.rule_batches.iter().try_fold(plan, |plan, batch| {
            self.optimize_with_rule_batch(batch, plan, &mut observer, &mut plan_tracker)
        })
    }

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
                        if plan_tracker.add_plan(new_plan.as_ref()) {
                            observer(new_plan.as_ref(), batch, pass, true, true);
                            ControlFlow::Continue(new_plan)
                        } else {
                            observer(new_plan.as_ref(), batch, pass, true, false);
                            ControlFlow::Break(Ok(new_plan))
                        }
                    }
                    Ok(Transformed::No(plan)) => {
                        observer(plan.as_ref(), batch, pass, false, false);
                        ControlFlow::Break(Ok(plan))
                    }
                    Err(e) => ControlFlow::Break(Err(e)),
                }
            },
        );
        match result {
            ControlFlow::Continue(result) => Ok(result),
            ControlFlow::Break(result) => result,
        }
    }

    pub fn optimize_with_rules(
        &self,
        rules: &[Box<dyn OptimizerRule>],
        plan: Arc<LogicalPlan>,
        order: &Option<ApplyOrder>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match order {
            Some(ApplyOrder::TopDown) => {
                // First optimize the current node, and then it's children.
                let curr_opt = self.optimize_node(rules, plan)?;
                let children_opt =
                    self.optimize_children(rules, curr_opt.unwrap().clone(), ApplyOrder::TopDown)?;
                Ok(children_opt.or(curr_opt))
            }
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

    fn optimize_node(
        &self,
        rules: &[Box<dyn OptimizerRule>],
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        rules.iter().try_fold(Transformed::No(plan), |plan, rule| {
            rule.try_optimize(plan.unwrap().clone())
        })
    }

    fn optimize_children(
        &self,
        rules: &[Box<dyn OptimizerRule>],
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

        Ok(Transformed::Yes(plan.with_new_children(&new_children)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use common_error::DaftResult;
    use daft_core::{datatypes::Field, schema::Schema, DataType};
    use daft_dsl::{col, lit};

    use crate::{
        ops::{Filter, Project},
        optimization::rules::{ApplyOrder, OptimizerRule, Transformed},
        test::dummy_scan_node,
        LogicalPlan,
    };

    use super::{Optimizer, OptimizerConfig, RuleBatch, RuleExecutionStrategy};

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
            LogicalPlan::from(dummy_scan_node(vec![Field::new("a", DataType::Int64)])).into();
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

    #[test]
    fn early_termination_equal_plan_cycle() -> DaftResult<()> {
        // Tests that the optimizer terminates early when a cycle is detected.
        // This test creates a Projection -> Source plan where the projection has [1, 2, 3] projections;
        // the optimizer will rotate the projection expressions to the left on each pass.
        //   [1, 2, 3] -> [2, 3, 1] -> [3, 1, 2] -> [1, 2, 3]
        // The optimization loop should therefore terminate on the 4th pass.
        let optimizer = Optimizer::with_rule_batches(
            vec![RuleBatch::new(
                vec![Box::new(RotateProjection::new(false))],
                RuleExecutionStrategy::FixedPoint(Some(20)),
            )],
            OptimizerConfig::new(20),
        );
        let plan: LogicalPlan = dummy_scan_node(vec![Field::new("a", DataType::Int64)]).into();
        let proj_exprs = vec![col("a") + lit(1), col("a") + lit(2), col("a") + lit(3)];
        let plan: LogicalPlan = Project::new(
            proj_exprs.clone(),
            Schema::new(
                proj_exprs
                    .iter()
                    .map(|e| {
                        Field::new(
                            e.semantic_id(plan.schema().as_ref()).to_string(),
                            DataType::Int64,
                        )
                    })
                    .collect(),
            )?
            .into(),
            Default::default(),
            plan.into(),
        )
        .into();
        let initial_plan: Arc<LogicalPlan> = plan.into();
        let mut pass_count = 0;
        let mut did_transform = false;
        optimizer.optimize(initial_plan.clone(), |_, _, _, transformed, _| {
            pass_count += 1;
            did_transform |= transformed;
        })?;
        assert!(did_transform);
        assert_eq!(pass_count, 3);
        Ok(())
    }

    #[test]
    fn early_termination_equal_plan_cycle_after_first() -> DaftResult<()> {
        // Tests that the optimizer terminates early when a cycle is detected.
        // This test creates a Projection -> Source plan where the projection has [1, 2, 3] projections;
        // the optimizer will reverse the projection expressions on the first pass and rotate them to the
        // left on each pass thereafter.
        //   [1, 2, 3] -> [3, 2, 1] -> [2, 1, 3] -> [1, 3, 2] -> [3, 2, 1]
        // The optimization loop should therefore terminate on the 5th pass.
        let optimizer = Optimizer::with_rule_batches(
            vec![RuleBatch::new(
                vec![Box::new(RotateProjection::new(true))],
                RuleExecutionStrategy::FixedPoint(Some(20)),
            )],
            OptimizerConfig::new(20),
        );
        let plan: LogicalPlan = dummy_scan_node(vec![Field::new("a", DataType::Int64)]).into();
        let proj_exprs = vec![col("a") + lit(1), col("a") + lit(2), col("a") + lit(3)];
        let plan: LogicalPlan = Project::new(
            proj_exprs.clone(),
            Schema::new(
                proj_exprs
                    .iter()
                    .map(|e| {
                        Field::new(
                            e.semantic_id(plan.schema().as_ref()).to_string(),
                            DataType::Int64,
                        )
                    })
                    .collect(),
            )?
            .into(),
            Default::default(),
            plan.into(),
        )
        .into();
        let initial_plan: Arc<LogicalPlan> = plan.into();
        let mut pass_count = 0;
        let mut did_transform = false;
        optimizer.optimize(initial_plan.clone(), |_, _, _, transformed, _| {
            pass_count += 1;
            did_transform |= transformed;
        })?;
        assert!(did_transform);
        assert_eq!(pass_count, 4);
        Ok(())
    }

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
                LogicalPlan::from(Project::new(
                    exprs,
                    project.projected_schema.clone(),
                    project.resource_request.clone(),
                    project.input.clone(),
                ))
                .into(),
            ))
        }
    }

    #[test]
    fn multiple_rule_batches() -> DaftResult<()> {
        // Tests that the optimizer applies multiple rule batches.
        // This test creates a Filter -> Projection -> Source plan and has 3 rule batches:
        // (1) ([NoOp, RotateProjection], FixedPoint(20)), which should terminate due to a plan cycle.
        // (2) ([FilterOrFalse, RotateProjection], FixedPoint(2)), which should run twice.
        // (3) ([FilterAndTrue], Once), which should run once.
        // The Projection has exprs [1, 2, 3], meaning that (1) should run 4 times before the cycle is hit.
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
        let plan: LogicalPlan = dummy_scan_node(vec![Field::new("a", DataType::Int64)]).into();
        let proj_exprs = vec![col("a") + lit(1), col("a") + lit(2), col("a") + lit(3)];
        let plan: LogicalPlan = Project::new(
            proj_exprs.clone(),
            Schema::new(
                proj_exprs
                    .iter()
                    .map(|e| {
                        Field::new(
                            e.semantic_id(plan.schema().as_ref()).to_string(),
                            DataType::Int64,
                        )
                    })
                    .collect(),
            )?
            .into(),
            Default::default(),
            plan.into(),
        )
        .into();
        let plan: LogicalPlan = Filter::new(col("a").lt(&lit(2)), plan.into()).into();
        let initial_plan: Arc<LogicalPlan> = plan.into();
        let mut pass_count = 0;
        let mut did_transform = false;
        let opt_plan = optimizer.optimize(initial_plan.clone(), |_, _, _, transformed, _| {
            pass_count += 1;
            did_transform |= transformed;
        })?;
        assert!(did_transform);
        // 4 + 2 + 1
        assert_eq!(pass_count, 6);
        let expected = "\
        Filter: [[[col(a) < lit(2)] | lit(false)] | lit(false)] & lit(true)\
        \n  Project: col(a) + lit(3), col(a) + lit(1), col(a) + lit(2)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64)";
        assert_eq!(opt_plan.repr_indent(), expected);
        Ok(())
    }

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
            let new_predicate = filter.predicate.or(&lit(false));
            Ok(Transformed::Yes(
                LogicalPlan::from(Filter::new(new_predicate, filter.input.clone())).into(),
            ))
        }
    }

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
            let new_predicate = filter.predicate.and(&lit(true));
            Ok(Transformed::Yes(
                LogicalPlan::from(Filter::new(new_predicate, filter.input.clone())).into(),
            ))
        }
    }
}
