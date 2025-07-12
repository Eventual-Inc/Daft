use std::{ops::ControlFlow, sync::Arc};

use common_error::DaftResult;
use common_treenode::Transformed;

use super::{
    logical_plan_tracker::LogicalPlanTracker,
    rules::{
        DetectMonotonicId, DropRepartition, EliminateCrossJoin, EliminateSubqueryAliasRule,
        EnrichWithStats, ExtractWindowFunction, FilterNullJoinKey, LiftProjectFromAgg,
        MaterializeScans, OptimizerRule, PushDownAntiSemiJoin, PushDownFilter,
        PushDownJoinPredicate, PushDownLimit, PushDownProjection, ReorderJoins,
        RewriteCountDistinct, SimplifyExpressionsRule, SimplifyNullFilteredJoin,
        SplitActorPoolProjects, SplitGranularProjection, UnnestPredicateSubquery,
        UnnestScalarSubquery,
    },
};
use crate::{
    optimization::rules::{PushDownShard, ShardScans, SplitExplodeFromProject},
    LogicalPlan,
};

/// Config for optimizer.
#[derive(Debug)]
pub struct OptimizerConfig {
    // Default maximum number of optimization passes the optimizer will make over a fixed-point RuleBatch.
    pub default_max_optimizer_passes: usize,
}

impl OptimizerConfig {
    fn new(max_optimizer_passes: usize) -> Self {
        Self {
            default_max_optimizer_passes: max_optimizer_passes,
        }
    }
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        // Default to a max of 5 optimizer passes for a given batch.
        Self::new(20)
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
}

impl RuleBatch {
    pub fn new(rules: Vec<Box<dyn OptimizerRuleInBatch>>, strategy: RuleExecutionStrategy) -> Self {
        // Get all unique application orders for the rules.
        Self { rules, strategy }
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

pub struct OptimizerBuilder {
    // Batches of rules for the optimizer to apply.
    rule_batches: Vec<RuleBatch>,
    // Config for optimizer.
    config: OptimizerConfig,
}

impl Default for OptimizerBuilder {
    fn default() -> Self {
        Self {
            rule_batches: vec![
                // --- Rewrite rules ---
                RuleBatch::new(
                    vec![
                        Box::new(LiftProjectFromAgg::new()),
                        Box::new(RewriteCountDistinct::new()),
                        Box::new(UnnestScalarSubquery::new()),
                        Box::new(UnnestPredicateSubquery::new()),
                        Box::new(EliminateSubqueryAliasRule::new()),
                        Box::new(ExtractWindowFunction::new()),
                        Box::new(SplitExplodeFromProject::new()),
                    ],
                    RuleExecutionStrategy::FixedPoint(None),
                ),
                // we want to simplify expressions first to make the rest of the rules easier
                RuleBatch::new(
                    vec![Box::new(SimplifyExpressionsRule::new())],
                    RuleExecutionStrategy::FixedPoint(None),
                ),
                // --- Filter out null join keys ---
                // This rule should be run once, before any filter pushdown rules.
                RuleBatch::new(
                    vec![Box::new(FilterNullJoinKey::new())],
                    RuleExecutionStrategy::Once,
                ),
                // --- Anti/semi join pushdowns ---
                // This needs to be separate from PushDownProjection and PushDownFilter
                // because otherwise they will just keep swapping places.
                // We ultimately do want filters to go before joins, so we run this rule before PushDownFilter.
                RuleBatch::new(
                    vec![Box::new(PushDownAntiSemiJoin::new())],
                    RuleExecutionStrategy::FixedPoint(None),
                ),
                // --- Bulk of our rules ---
                RuleBatch::new(
                    vec![
                        Box::new(DropRepartition::new()),
                        Box::new(PushDownFilter::new()),
                        Box::new(PushDownProjection::new()),
                        Box::new(EliminateCrossJoin::new()),
                        Box::new(SimplifyNullFilteredJoin::new()),
                        Box::new(PushDownJoinPredicate::new()),
                    ],
                    // Use a fixed-point policy for the pushdown rules: PushDownProjection can produce a Filter node
                    // at the current node, which would require another batch application in order to have a chance to push
                    // that Filter node through upstream nodes.
                    // TODO(Clark): Refine this fixed-point policy.
                    RuleExecutionStrategy::FixedPoint(None),
                ),
                // --- Limit pushdowns ---
                // This needs to be separate from PushDownProjection because otherwise the limit and
                // projection just keep swapping places, preventing optimization
                // (see https://github.com/Eventual-Inc/Daft/issues/2616)
                RuleBatch::new(
                    vec![Box::new(PushDownLimit::new())],
                    RuleExecutionStrategy::FixedPoint(Some(3)),
                ),
                // --- Rewrite projections ---
                // Once optimization rules have been applied,split actor pool projects and detect monotonic IDs.
                // By delaying these rewrite rules, we avoid having to special case optimization rules for
                // actor pool projects and monotonically increasing ids.
                RuleBatch::new(
                    vec![
                        Box::new(SplitActorPoolProjects::new()),
                        Box::new(DetectMonotonicId::new()),
                    ],
                    RuleExecutionStrategy::Once,
                ),
                // Push down projections after rewriting projections.
                RuleBatch::new(
                    vec![Box::new(PushDownProjection::new())],
                    RuleExecutionStrategy::FixedPoint(None),
                ),
                // --- Shard pushdowns ---
                RuleBatch::new(
                    vec![Box::new(PushDownShard::new())],
                    RuleExecutionStrategy::Once,
                ),
                // --- Simplify expressions before scans are materialized ---
                RuleBatch::new(
                    vec![Box::new(SimplifyExpressionsRule::new())],
                    RuleExecutionStrategy::FixedPoint(None),
                ),
                // --- Materialize scan nodes ---
                RuleBatch::new(
                    vec![Box::new(MaterializeScans::new())],
                    RuleExecutionStrategy::Once,
                ),
                // --- Shard scans ---
                RuleBatch::new(
                    vec![Box::new(ShardScans::new())],
                    RuleExecutionStrategy::Once,
                ),
                // --- Enrich logical plan with stats ---
                RuleBatch::new(
                    vec![Box::new(EnrichWithStats::new())],
                    RuleExecutionStrategy::Once,
                ),
            ],
            config: Default::default(),
        }
    }
}

impl OptimizerBuilder {
    pub fn new() -> Self {
        Self {
            rule_batches: vec![],
            config: Default::default(),
        }
    }

    pub fn reorder_joins(mut self) -> Self {
        self.rule_batches.push(RuleBatch::new(
            vec![
                Box::new(ReorderJoins::new()),
                Box::new(PushDownFilter::new()),
                Box::new(PushDownProjection::new()),
                Box::new(EnrichWithStats::new()),
            ],
            RuleExecutionStrategy::Once,
        ));
        self
    }

    pub fn enrich_with_stats(mut self) -> Self {
        self.rule_batches.push(RuleBatch::new(
            vec![Box::new(EnrichWithStats::new())],
            RuleExecutionStrategy::Once,
        ));
        self
    }

    pub fn simplify_expressions(mut self) -> Self {
        // Try to simplify expressions again as other rules could introduce new exprs.
        self.rule_batches.push(RuleBatch::new(
            vec![Box::new(SimplifyExpressionsRule::new())],
            RuleExecutionStrategy::FixedPoint(None),
        ));
        self
    }

    pub fn split_granular_projections(mut self) -> Self {
        self.rule_batches.push(RuleBatch::new(
            vec![Box::new(SplitGranularProjection::new())],
            RuleExecutionStrategy::Once,
        ));
        self
    }

    pub fn with_optimizer_config(mut self, config: OptimizerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn build(self) -> Optimizer {
        Optimizer {
            rule_batches: self.rule_batches,
            config: self.config,
        }
    }

    pub fn when(self, condition: bool, f: impl FnOnce(Self) -> Self) -> Self {
        if condition {
            f(self)
        } else {
            self
        }
    }
}

/// Logical rule-based optimizer.
pub struct Optimizer {
    // Batches of rules for the optimizer to apply.
    pub rule_batches: Vec<RuleBatch>,
    // Config for optimizer.
    config: OptimizerConfig,
}

impl Optimizer {
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
                match self.optimize_with_rules(batch.rules.as_slice(), plan) {
                    Ok(Transformed {
                        data: new_plan,
                        transformed: true,
                        ..
                    }) => {
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
                    Ok(Transformed {
                        data: plan,
                        transformed: false,
                        ..
                    }) => {
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

    /// Optimize the provided plan with each of the provided rules once.
    pub fn optimize_with_rules(
        &self,
        rules: &[Box<dyn OptimizerRuleInBatch>],
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // Fold over the rules, applying each rule to this plan node sequentially.
        rules.iter().try_fold(Transformed::no(plan), |plan, rule| {
            plan.transform_data(|data| rule.try_optimize(data))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use common_error::DaftResult;
    use common_scan_info::Pushdowns;
    use common_treenode::{Transformed, TreeNode};
    use daft_core::prelude::*;
    use daft_dsl::{
        functions::{python::PythonUDF, FunctionExpr},
        lit, resolved_col, unresolved_col, Expr,
    };

    use super::{Optimizer, OptimizerBuilder, OptimizerConfig, RuleBatch, RuleExecutionStrategy};
    use crate::{
        ops::{ActorPoolProject, Filter, Project},
        optimization::rules::{EnrichWithStats, MaterializeScans, OptimizerRule},
        test::{dummy_scan_node, dummy_scan_node_with_pushdowns, dummy_scan_operator},
        LogicalPlan,
    };

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
        fn try_optimize(
            &self,
            plan: Arc<LogicalPlan>,
        ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
            Ok(Transformed::no(plan))
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
            unresolved_col("a").add(lit(1)),
            unresolved_col("a").add(lit(2)).alias("b"),
            unresolved_col("a").add(lit(3)).alias("c"),
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
            unresolved_col("a").add(lit(1)),
            unresolved_col("a").add(lit(2)).alias("b"),
            unresolved_col("a").add(lit(3)).alias("c"),
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
            unresolved_col("a").add(lit(1)),
            unresolved_col("a").add(lit(2)).alias("b"),
            unresolved_col("a").add(lit(3)).alias("c"),
        ];
        let filter_predicate = unresolved_col("a").lt(lit(2));
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

        let mut new_proj_exprs = proj_exprs;
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
        fn try_optimize(
            &self,
            plan: Arc<LogicalPlan>,
        ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
            plan.transform_down(|node| {
                let filter = match node.as_ref() {
                    LogicalPlan::Filter(filter) => filter.clone(),
                    _ => return Ok(Transformed::no(node)),
                };
                let new_predicate = filter.predicate.or(lit(false));
                Ok(Transformed::yes(
                    LogicalPlan::from(Filter::try_new(filter.input, new_predicate)?).into(),
                ))
            })
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
        fn try_optimize(
            &self,
            plan: Arc<LogicalPlan>,
        ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
            plan.transform_down(|node| {
                let filter = match node.as_ref() {
                    LogicalPlan::Filter(filter) => filter.clone(),
                    _ => return Ok(Transformed::no(node)),
                };
                let new_predicate = filter.predicate.and(lit(true));
                Ok(Transformed::yes(
                    LogicalPlan::from(Filter::try_new(filter.input, new_predicate)?).into(),
                ))
            })
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
        fn try_optimize(
            &self,
            plan: Arc<LogicalPlan>,
        ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
            plan.transform_down(|node| {
                let project = match node.as_ref() {
                    LogicalPlan::Project(project) => project.clone(),
                    _ => return Ok(Transformed::no(node)),
                };
                let mut exprs = project.projection.clone();
                let mut reverse = self.reverse_first.lock().unwrap();
                if *reverse {
                    exprs.reverse();
                    *reverse = false;
                } else {
                    exprs.rotate_left(1);
                }
                Ok(Transformed::yes(
                    LogicalPlan::from(Project::try_new(project.input, exprs)?).into(),
                ))
            })
        }
    }

    fn get_scan_materializer_and_stats_enricher() -> Optimizer {
        Optimizer::with_rule_batches(
            vec![RuleBatch::new(
                vec![
                    Box::new(MaterializeScans::new()),
                    Box::new(EnrichWithStats::new()),
                ],
                RuleExecutionStrategy::Once,
            )],
            OptimizerConfig::new(5),
        )
    }

    /// Tests that Limit commutes with ActorPoolProject.
    ///
    /// Limit-ActorPoolProject-Source -> ActorPoolProject-Source[with_limit]
    #[test]
    fn limit_commutes_with_actor_pool_project() -> DaftResult<()> {
        let limit = 5;
        let scan_op = dummy_scan_operator(vec![Field::new("a", DataType::Int64)]);

        // Create an actor pool project expression.
        let actor_pool_expr = Arc::new(Expr::Function {
            func: FunctionExpr::Python(PythonUDF::new_testing_udf()),
            inputs: vec![resolved_col("a").into()],
        });

        // Create a plan with Select using actor pool project followed by Limit.
        let plan = dummy_scan_node(scan_op.clone())
            .select(vec![actor_pool_expr.clone()])?
            .limit(limit, false)?
            .build();

        // Expected optimized plan should have Limit pushed down.
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(limit as usize)),
        )
        .limit(limit, false)?
        .build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected.clone(),
            // Internally, splitting an actor pool project always re-aliases the column to its original name.
            vec![actor_pool_expr.alias("a")],
        )?)
        .arced();
        let scan_materializer_and_stats_enricher = get_scan_materializer_and_stats_enricher();
        let expected = scan_materializer_and_stats_enricher
            .optimize_with_rules(
                scan_materializer_and_stats_enricher.rule_batches[0]
                    .rules
                    .as_slice(),
                expected,
            )?
            .data;

        // Check that the limit commutes with the actor pool project.
        let optimizer = OptimizerBuilder::default().build();
        let opt_plan = optimizer.optimize(plan, |_, _, _, _, _| {})?;
        assert_eq!(
            opt_plan,
            expected,
            "\n\nOptimized plan not equal to expected.\n\nOptimized:\n{}\n\nExpected:\n{}",
            opt_plan.repr_ascii(false),
            expected.repr_ascii(false)
        );
        Ok(())
    }

    /// Tests that Filter commutes with ActorPoolProject.
    ///
    /// Filter-ActorPoolProject-Source -> ActorPoolProject-Source[with_filter]
    #[test]
    fn filter_commutes_with_actor_pool_project() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", DataType::Int64)]);

        // Create an actor pool project expression.
        let actor_pool_expr = Arc::new(Expr::Function {
            func: FunctionExpr::Python(PythonUDF::new_testing_udf()),
            inputs: vec![resolved_col("a").into()],
        });

        // Create a plan with Select using actor pool project followed by Filter.
        let plan = dummy_scan_node(scan_op.clone())
            .select(vec![
                resolved_col("a"),
                actor_pool_expr.clone().alias("renamed_col"),
            ])?
            .filter(resolved_col("a").lt(lit(2)))?
            .build();

        // Expected optimized plan should have Filter pushed down.
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_filters(Some(resolved_col("a").lt(lit(2)))),
        )
        .build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected.clone(),
            // Internally, splitting an actor pool project always re-aliases the column to its original name.
            vec![resolved_col("a"), actor_pool_expr.alias("renamed_col")],
        )?)
        .arced();
        let scan_materializer_and_stats_enricher = get_scan_materializer_and_stats_enricher();
        let expected = scan_materializer_and_stats_enricher
            .optimize_with_rules(
                scan_materializer_and_stats_enricher.rule_batches[0]
                    .rules
                    .as_slice(),
                expected,
            )?
            .data;

        // Check that the filter commutes with the actor pool project.
        let optimizer = OptimizerBuilder::default().build();
        let opt_plan = optimizer.optimize(plan, |_, _, _, _, _| {})?;
        assert_eq!(
            opt_plan,
            expected,
            "\n\nOptimized plan not equal to expected.\n\nOptimized:\n{}\n\nExpected:\n{}",
            opt_plan.repr_ascii(false),
            expected.repr_ascii(false)
        );
        Ok(())
    }

    // TODO(desmond): Add tests that Filter and Limit commutes with monotonically increasing ids.
    // We can't do this at the momente currently rewrite monotonically increasing ids to a
    // monotonically increasing id column node that produces a column with a uuid. Moving away from
    // string-based column matching will make this test easier to write.
}
