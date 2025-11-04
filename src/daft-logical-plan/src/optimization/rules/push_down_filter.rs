use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::DaftResult;
use common_scan_info::{PredicateGroups, ScanState, rewrite_predicate_for_partitioning};
use common_treenode::{DynTreeNode, Transformed, TreeNode};
use daft_algebra::boolean::{combine_conjunction, split_conjunction, to_cnf};
use daft_dsl::{
    ExprRef,
    optimization::{get_required_columns, replace_columns_with_expressions},
    resolved_col,
};

use super::OptimizerRule;
use crate::{
    LogicalPlan,
    ops::{Concat, Filter, Join, Project, Source},
    source_info::SourceInfo,
};

/// Optimization rules for pushing Filters further into the logical plan.
#[derive(Default, Debug)]
pub struct PushDownFilter {
    strict_pushdown: bool,
}

impl PushDownFilter {
    pub fn new(strict_pushdown: bool) -> Self {
        Self { strict_pushdown }
    }
}

impl OptimizerRule for PushDownFilter {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| self.try_optimize_node(node))
    }
}

impl PushDownFilter {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let filter = match plan.as_ref() {
            LogicalPlan::Filter(filter) => filter,
            _ => return Ok(Transformed::no(plan)),
        };
        let child_plan = filter.input.as_ref();
        let new_plan = match child_plan {
            LogicalPlan::Filter(child_filter) => {
                // Combine filters.
                //
                // Filter-Filter --> Filter

                // Split predicate expression on conjunctions (ANDs).
                let parent_predicates = split_conjunction(&filter.predicate);
                let predicate_set: HashSet<&ExprRef> = parent_predicates.iter().collect();
                // Add child predicate expressions to parent predicate expressions, eliminating duplicates.
                let new_predicates: Vec<ExprRef> = parent_predicates
                    .iter()
                    .chain(
                        split_conjunction(&child_filter.predicate)
                            .iter()
                            .filter(|e| !predicate_set.contains(*e)),
                    )
                    .map(|e| (*e).clone())
                    .collect::<Vec<_>>();
                // Reconjunct predicate expressions.
                let new_predicate = combine_conjunction(new_predicates).unwrap();
                let new_filter: Arc<LogicalPlan> =
                    LogicalPlan::from(Filter::try_new(child_filter.input.clone(), new_predicate)?)
                        .into();
                self.try_optimize_node(new_filter.clone())?
                    .or(Transformed::yes(new_filter))
                    .data
            }
            LogicalPlan::Source(source) => {
                match source.source_info.as_ref() {
                    // Filter pushdown is not supported for in-memory sources.
                    SourceInfo::InMemory(_) => return Ok(Transformed::no(plan)),
                    // Filter pushdown is not supported for glob scan sources.
                    SourceInfo::GlobScan(_) => return Ok(Transformed::no(plan)),
                    // Do not pushdown if Source node already has a limit
                    SourceInfo::Physical(external_info)
                        if let Some(_) = external_info.pushdowns.limit =>
                    {
                        return Ok(Transformed::no(plan));
                    }

                    // Pushdown filter into the Source node
                    SourceInfo::Physical(external_info) => {
                        // If the scan is materialized, we don't pushdown the filter.
                        if matches!(external_info.scan_state, ScanState::Tasks(_)) {
                            return Ok(Transformed::no(plan));
                        }
                        let predicate = &filter.predicate;
                        let new_predicate = external_info
                            .pushdowns
                            .filters
                            .as_ref()
                            .map(|f| predicate.clone().and(f.clone()))
                            .unwrap_or_else(|| predicate.clone());
                        // We split the predicate into three groups:
                        // 1. All partition-only filters, which can be applied directly to partition values and can be
                        //    dropped from the data-level filter.
                        // 2. Predicates that only reference data columns (no partition column references) or only
                        //    reference partition columns but involve non-identity transformations; these need to be
                        //    applied to the data, but don't require a separate filter op (i.e. they can be pushed into
                        //    the scan).
                        // 3. Filters needing their own dedicated filter op (unable to be pushed into scan); this
                        //    includes predicates involving both partition and data columns, and predicates containing
                        //    UDFs.
                        let PredicateGroups {
                            partition_only_filter,
                            data_only_filter,
                            needing_filter_op,
                        } = rewrite_predicate_for_partitioning(
                            &new_predicate,
                            external_info.scan_state.get_scan_op().0.partitioning_keys(),
                        )?;
                        assert!(
                            partition_only_filter.len()
                                + data_only_filter.len()
                                + needing_filter_op.len()
                                > 0
                        );

                        if !needing_filter_op.is_empty()
                            && partition_only_filter.is_empty()
                            && data_only_filter.is_empty()
                        {
                            // If the filter predicate consists of only expressions that rely on both a partition
                            // column and a data column (or contain a UDF), then no pushdown into the scan is possible,
                            // so we short-circuit.
                            // TODO(Clark): Support pushing predicates referencing both partition and data columns into the scan.
                            return Ok(Transformed::no(plan));
                        }

                        //deduplicate
                        let data_only_filter = {
                            let mut seen = HashSet::new();
                            data_only_filter
                                .into_iter()
                                .filter(|e| seen.insert(e.clone()))
                                .collect::<Vec<_>>()
                        };

                        let data_filter = combine_conjunction(data_only_filter.clone());
                        let partition_filter = combine_conjunction(partition_only_filter);
                        assert!(data_filter.is_some() || partition_filter.is_some());

                        let new_pushdowns = if let Some(data_filter) = data_filter {
                            external_info.pushdowns.with_filters(Some(data_filter))
                        } else {
                            external_info.pushdowns.clone()
                        };
                        let new_pushdowns = if let Some(partition_filter) = partition_filter {
                            new_pushdowns.with_partition_filters(Some(partition_filter))
                        } else {
                            new_pushdowns
                        };

                        let scan_op = external_info.scan_state.get_scan_op().0.clone();
                        let remaining_filters = if let Some(supports_pushdown) =
                            scan_op.as_pushdown_filter()
                            && self.strict_pushdown
                        {
                            let filters_to_push = new_pushdowns.filters.as_slice();

                            let (pushed_filters, post_filters) =
                                supports_pushdown.push_filters(filters_to_push);
                            let _ = new_pushdowns.with_pushed_filters(Some(pushed_filters.clone()));
                            if !post_filters.is_empty()
                                && post_filters.len() == filters_to_push.len()
                            {
                                return Ok(Transformed::no(plan));
                            }

                            let mut seen = HashSet::new();
                            let pushed_filters_set = if pushed_filters.len() == 1 {
                                split_conjunction(&pushed_filters[0])
                                    .into_iter()
                                    .collect::<HashSet<_>>()
                            } else {
                                pushed_filters
                                    .iter()
                                    .flat_map(split_conjunction)
                                    .collect::<HashSet<_>>()
                            };
                            post_filters
                                .into_iter()
                                .chain(
                                    data_only_filter
                                        .iter()
                                        .filter(|f| !pushed_filters_set.contains(*f))
                                        .cloned(),
                                )
                                .filter(|e| seen.insert(e.clone()))
                                .collect::<Vec<_>>()
                        } else {
                            // Return an empty Vec if strict pushdown is not supported
                            Vec::new()
                        };

                        let needing_filter_op = {
                            let mut seen = HashSet::new();
                            remaining_filters
                                .into_iter()
                                .chain(needing_filter_op)
                                .filter(|e| seen.insert(e.clone()))
                                .collect::<Vec<_>>()
                        };

                        let new_external_info = external_info.with_pushdowns(new_pushdowns);
                        let new_source: LogicalPlan = Source::new(
                            source.output_schema.clone(),
                            SourceInfo::Physical(new_external_info).into(),
                        )
                        .into();
                        if !needing_filter_op.is_empty() {
                            // We need to apply any filter predicates that reference both partition and data columns after the scan.
                            // TODO(Clark): Support pushing predicates referencing both partition and data columns into the scan.
                            let filter_op: LogicalPlan = Filter::try_new(
                                new_source.into(),
                                combine_conjunction(needing_filter_op).unwrap(),
                            )?
                            .into();
                            return Ok(Transformed::yes(filter_op.into()));
                        } else {
                            return Ok(Transformed::yes(new_source.into()));
                        }
                    }
                    SourceInfo::PlaceHolder(..) => {
                        panic!("PlaceHolderInfo should not exist for optimization!");
                    }
                }
            }
            LogicalPlan::Project(child_project) => {
                // Commute filter with projection if predicate only depends on projection columns that
                // don't involve compute.
                //
                // Filter-Projection --> {Filter-}Projection-Filter
                let predicates = split_conjunction(&filter.predicate);
                let projection_input_mapping = child_project
                    .projection
                    .iter()
                    .filter_map(|e| {
                        e.input_mapping()
                            .map(|s| (e.name().to_string(), resolved_col(s)))
                    })
                    .collect::<HashMap<String, ExprRef>>();
                // Split predicate expressions into those that don't depend on projection compute (can_push) and those
                // that do (can_not_push).
                // TODO(Clark): Push Filters depending on Projection columns involving compute if those expressions are
                // (1) deterministic && (pure || idempotent),
                // (2) inexpensive to recompute.
                // This can be done by rewriting the Filter predicate expression to contain the relevant Projection expression.
                let mut can_push: Vec<ExprRef> = vec![];
                let mut can_not_push: Vec<ExprRef> = vec![];
                for predicate in predicates {
                    let predicate_cols = get_required_columns(&predicate);
                    if predicate_cols
                        .iter()
                        .all(|col| projection_input_mapping.contains_key(col))
                    {
                        // Can push predicate through expression.
                        let new_predicate = replace_columns_with_expressions(
                            predicate.clone(),
                            &projection_input_mapping,
                        );
                        can_push.push(new_predicate);
                    } else {
                        // Can't push predicate expression through projection.
                        can_not_push.push(predicate.clone());
                    }
                }
                if can_push.is_empty() {
                    // No predicate expressions can be pushed through projection.
                    return Ok(Transformed::no(plan));
                }
                // Create new Filter with predicates that can be pushed past Projection.
                let predicates_to_push = combine_conjunction(can_push).unwrap();
                let push_down_filter: LogicalPlan =
                    Filter::try_new(child_project.input.clone(), predicates_to_push)?.into();
                // Create new Projection.
                let new_projection: LogicalPlan =
                    Project::try_new(push_down_filter.into(), child_project.projection.clone())?
                        .into();
                if can_not_push.is_empty() {
                    // If all Filter predicate expressions were pushable past Projection, return new
                    // Projection-Filter subplan.
                    new_projection.into()
                } else {
                    // Otherwise, add a Filter after Projection that filters with predicate expressions
                    // that couldn't be pushed past the Projection, returning a Filter-Projection-Filter subplan.
                    let post_projection_predicate = combine_conjunction(can_not_push).unwrap();
                    let post_projection_filter: LogicalPlan =
                        Filter::try_new(new_projection.into(), post_projection_predicate)?.into();
                    post_projection_filter.into()
                }
            }
            LogicalPlan::Sort(_) | LogicalPlan::Repartition(_) | LogicalPlan::IntoBatches(_) => {
                // Naive commuting with unary ops.
                let new_filter = plan
                    .with_new_children(&[child_plan.arc_children()[0].clone()])
                    .into();
                child_plan.with_new_children(&[new_filter]).into()
            }
            LogicalPlan::Concat(Concat { input, other, .. }) => {
                // Push filter into each side of the concat.
                let new_input: LogicalPlan =
                    Filter::try_new(input.clone(), filter.predicate.clone())?.into();
                let new_other: LogicalPlan =
                    Filter::try_new(other.clone(), filter.predicate.clone())?.into();
                let new_concat: LogicalPlan =
                    Concat::try_new(new_input.into(), new_other.into())?.into();
                new_concat.into()
            }
            LogicalPlan::Join(Join {
                left,
                right,
                join_type,
                on,
                join_strategy,
                ..
            }) => {
                // TODO(Kevin): add more filter pushdowns for joins
                // Example:
                //      For `foo JOIN bar ON foo.a == (bar.b + 2) WHERE a > 0`, the filter `a > 0` is pushed down to the left side, but can also be pushed down to the right side as `(b + 2) > 0`

                let mut left_pushdowns = vec![];
                let mut right_pushdowns = vec![];
                let mut kept_predicates = vec![];

                let left_cols = HashSet::<_>::from_iter(left.schema().names());
                let right_cols = HashSet::<_>::from_iter(right.schema().names());

                // TODO: simplify predicates, since they may be expanded with `to_cnf`
                for predicate in split_conjunction(&to_cnf(filter.predicate.clone())) {
                    let pred_cols = HashSet::<_>::from_iter(get_required_columns(&predicate));

                    match (
                        pred_cols.is_subset(&left_cols),
                        pred_cols.is_subset(&right_cols),
                    ) {
                        (true, true) => {
                            // predicate only depends on common join keys, so we can push it down to both sides
                            left_pushdowns.push(predicate.clone());
                            right_pushdowns.push(predicate);
                        }
                        (false, false) => {
                            // predicate depends on unique columns on both left and right sides, so we can't push it down
                            kept_predicates.push(predicate);
                        }
                        (true, false) => {
                            if join_type.left_produces_nulls() {
                                kept_predicates.push(predicate);
                            } else {
                                left_pushdowns.push(predicate);
                            }
                        }
                        (false, true) => {
                            if join_type.right_produces_nulls() {
                                kept_predicates.push(predicate);
                            } else {
                                right_pushdowns.push(predicate);
                            }
                        }
                    }
                }

                let left_pushdowns = combine_conjunction(left_pushdowns);
                let right_pushdowns = combine_conjunction(right_pushdowns);

                if left_pushdowns.is_some() || right_pushdowns.is_some() {
                    let kept_predicates = combine_conjunction(kept_predicates);

                    let new_left = left_pushdowns.map_or_else(
                        || left.clone(),
                        |left_pushdowns| {
                            Filter::try_new(left.clone(), left_pushdowns)
                                .unwrap()
                                .into()
                        },
                    );

                    let new_right = right_pushdowns.map_or_else(
                        || right.clone(),
                        |right_pushdowns| {
                            Filter::try_new(right.clone(), right_pushdowns)
                                .unwrap()
                                .into()
                        },
                    );

                    let new_join = Arc::new(LogicalPlan::Join(Join::try_new(
                        new_left,
                        new_right,
                        on.clone(),
                        *join_type,
                        *join_strategy,
                    )?));

                    if let Some(kept_predicates) = kept_predicates {
                        Filter::try_new(new_join, kept_predicates).unwrap().into()
                    } else {
                        new_join
                    }
                } else {
                    return Ok(Transformed::no(plan));
                }
            }
            LogicalPlan::Limit(_)
            | LogicalPlan::Offset(_)
            | LogicalPlan::TopN(..)
            | LogicalPlan::Sample(..)
            | LogicalPlan::Explode(..)
            | LogicalPlan::Shard(..)
            | LogicalPlan::UDFProject(..)
            | LogicalPlan::Unpivot(..)
            | LogicalPlan::Pivot(..)
            | LogicalPlan::Aggregate(..)
            | LogicalPlan::Intersect(..)
            | LogicalPlan::Union(..)
            | LogicalPlan::Sink(..)
            | LogicalPlan::MonotonicallyIncreasingId(..)
            | LogicalPlan::SubqueryAlias(..)
            | LogicalPlan::Window(..)
            | LogicalPlan::Distinct(..)
            | LogicalPlan::VLLMProject(..) => {
                return Ok(Transformed::no(plan));
            }
        };
        Ok(Transformed::yes(new_plan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use common_scan_info::Pushdowns;
    use daft_core::prelude::*;
    use daft_dsl::{ExprRef, functions::BuiltinScalarFn, lit, resolved_col, unresolved_col};
    use daft_functions_uri::download::UrlDownload;
    use rstest::rstest;

    use crate::{
        LogicalPlan,
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::PushDownFilter,
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_node_with_pushdowns, dummy_scan_operator},
    };

    /// Helper that creates an optimizer with the PushDownFilter rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
        strict_pushdown: bool,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(PushDownFilter::new(strict_pushdown))],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    /// Tests that we can't pushdown a filter into a ScanOperator that has a limit.
    #[test]
    fn filter_not_pushed_down_into_scan_with_limit() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan =
            dummy_scan_node_with_pushdowns(scan_op, Pushdowns::default().with_limit(Some(1)))
                .filter(resolved_col("a").lt(lit(2)))?
                .build();
        // Plan should be unchanged after optimization.
        let expected = plan.clone();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests combining of two Filters by merging their predicates.
    #[rstest]
    fn filter_combine_with_filter(#[values(false, true)] push_into_scan: bool) -> DaftResult<()> {
        use daft_dsl::resolved_col;

        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_scan { None } else { Some(1) }),
        );
        let p1 = resolved_col("a").lt(lit(2));
        let p2 = resolved_col("b").eq(lit("foo"));
        let plan = scan_plan.filter(p1.clone())?.filter(p2.clone())?.build();
        let merged_filter = p2.and(p1);
        let expected = if push_into_scan {
            // Merged filter should be pushed into scan.
            dummy_scan_node_with_pushdowns(
                scan_op,
                Pushdowns::default().with_filters(Some(merged_filter)),
            )
            .build()
        } else {
            // Merged filter should not be pushed into scan.
            scan_plan.filter(merged_filter)?.build()
        };
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that we can't pushdown a filter into a ScanOperator if it has an udf-ish expression.
    #[test]
    fn filter_with_udf_not_pushed_down_into_scan() -> DaftResult<()> {
        let pred: ExprRef = BuiltinScalarFn::new_async(
            UrlDownload,
            vec![resolved_col("a"), lit(1), lit(true), lit(true)],
        )
        .into();
        let plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]))
        .filter(pred.is_null())?
        .build();
        let expected = plan.clone();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter commutes with Projections.
    #[rstest]
    fn filter_commutes_with_projection(
        #[values(false, true)] push_into_scan: bool,
    ) -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_scan { None } else { Some(1) }),
        );
        let pred = resolved_col("a").lt(lit(2));
        let proj = vec![resolved_col("a")];
        let plan = scan_plan
            .select(proj.clone())?
            .filter(pred.clone())?
            .build();
        let expected_scan_filter = if push_into_scan {
            dummy_scan_node_with_pushdowns(scan_op, Pushdowns::default().with_filters(Some(pred)))
        } else {
            scan_plan.filter(pred)?
        };
        let expected = expected_scan_filter.select(proj)?.build();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that a Filter with multiple columns in its predicate commutes with a Projection on both of those columns.
    #[rstest]
    fn filter_commutes_with_projection_multi(
        #[values(false, true)] push_into_scan: bool,
    ) -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_scan { None } else { Some(1) }),
        );
        let pred = resolved_col("a")
            .lt(lit(2))
            .and(resolved_col("b").eq(lit("foo")));
        let proj = vec![resolved_col("a"), resolved_col("b")];
        let plan = scan_plan
            .select(proj.clone())?
            .filter(pred.clone())?
            .build();
        let expected_scan_filter = if push_into_scan {
            dummy_scan_node_with_pushdowns(scan_op, Pushdowns::default().with_filters(Some(pred)))
        } else {
            scan_plan.filter(pred)?
        };
        let expected = expected_scan_filter.select(proj)?.build();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter does not commute with a Projection if the projection expression involves compute.
    #[test]
    fn filter_does_not_commute_with_projection_if_compute() -> DaftResult<()> {
        let plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]))
        // Projection involves compute on filtered column "a".
        .select(vec![resolved_col("a").add(lit(1))])?
        .filter(resolved_col("a").lt(lit(2)))?
        .build();
        // Filter should NOT commute with Project, since this would involve redundant computation.
        let expected = plan.clone();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter commutes with Projection if projection expression involves deterministic compute.
    // REASON - No expression attribute indicating whether deterministic && (pure || idempotent).
    #[ignore]
    #[rstest]
    fn filter_commutes_with_projection_deterministic_compute(
        #[values(false, true)] push_into_scan: bool,
    ) -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_scan { None } else { Some(1) }),
        );
        let pred = resolved_col("a").lt(lit(2));
        let proj = vec![resolved_col("a").add(lit(1))];
        let plan = scan_plan
            // Projection involves compute on filtered column "a".
            .select(proj.clone())?
            .filter(pred.clone())?
            .build();
        let expected_filter_scan = if push_into_scan {
            dummy_scan_node_with_pushdowns(scan_op, Pushdowns::default().with_filters(Some(pred)))
        } else {
            scan_plan.filter(pred)?
        };
        let expected = expected_filter_scan.select(proj)?.build();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter commutes with Sort.
    #[rstest]
    fn filter_commutes_with_sort(#[values(false, true)] push_into_scan: bool) -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_scan { None } else { Some(1) }),
        );
        let pred = resolved_col("a").lt(lit(2));
        let sort_by = vec![resolved_col("a")];
        let descending = vec![true];
        let nulls_first = vec![false];
        let plan = scan_plan
            .sort(sort_by.clone(), descending.clone(), nulls_first.clone())?
            .filter(pred.clone())?
            .build();
        let expected_filter_scan = if push_into_scan {
            dummy_scan_node_with_pushdowns(scan_op, Pushdowns::default().with_filters(Some(pred)))
        } else {
            scan_plan.filter(pred)?
        };
        let expected = expected_filter_scan
            .sort(sort_by, descending, nulls_first)?
            .build();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter commutes with Repartition.
    #[rstest]
    fn filter_commutes_with_repartition(
        #[values(false, true)] push_into_scan: bool,
    ) -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_scan { None } else { Some(1) }),
        );
        let pred = resolved_col("a").lt(lit(2));
        let num_partitions = 1;
        let repartition_by = vec![resolved_col("a")];
        let plan = scan_plan
            .hash_repartition(Some(num_partitions), repartition_by.clone())?
            .filter(pred.clone())?
            .build();
        let expected_filter_scan = if push_into_scan {
            dummy_scan_node_with_pushdowns(scan_op, Pushdowns::default().with_filters(Some(pred)))
        } else {
            scan_plan.filter(pred)?
        };
        let expected = expected_filter_scan
            .hash_repartition(Some(num_partitions), repartition_by)?
            .build();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter commutes with IntoBatches.
    #[rstest]
    fn filter_commutes_with_into_batches(
        #[values(false, true)] push_into_scan: bool,
    ) -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_scan { None } else { Some(1) }),
        );
        let pred = resolved_col("a").lt(lit(2));
        let batch_size = 5usize;
        let plan = scan_plan
            .into_batches(batch_size)?
            .filter(pred.clone())?
            .build();
        let expected_filter_scan = if push_into_scan {
            dummy_scan_node_with_pushdowns(scan_op, Pushdowns::default().with_filters(Some(pred)))
        } else {
            scan_plan.filter(pred)?
        };
        let expected = expected_filter_scan.into_batches(batch_size)?.build();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter commutes with Concat.
    #[rstest]
    fn filter_commutes_with_concat(
        #[values(false, true)] push_into_left_scan: bool,
        #[values(false, true)] push_into_right_scan: bool,
    ) -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let left_scan_plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_left_scan { None } else { Some(1) }),
        );
        let right_scan_plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_right_scan { None } else { Some(1) }),
        );
        let pred = resolved_col("a").lt(lit(2));
        let plan = left_scan_plan
            .concat(&right_scan_plan)?
            .filter(pred.clone())?
            .build();
        let expected_left_filter_scan = if push_into_left_scan {
            dummy_scan_node_with_pushdowns(
                scan_op.clone(),
                Pushdowns::default().with_filters(Some(pred.clone())),
            )
        } else {
            left_scan_plan.filter(pred.clone())?
        };
        let expected_right_filter_scan = if push_into_right_scan {
            dummy_scan_node_with_pushdowns(scan_op, Pushdowns::default().with_filters(Some(pred)))
        } else {
            right_scan_plan.filter(pred)?
        };
        let expected = expected_left_filter_scan
            .concat(&expected_right_filter_scan)?
            .build();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter can be pushed into the left side of a Join.
    #[rstest]
    fn filter_commutes_with_join_left_side(
        #[values(false, true)] push_into_left_scan: bool,
        #[values(false, true)] null_equals_null: bool,
        #[values(JoinType::Inner, JoinType::Left, JoinType::Anti, JoinType::Semi)] how: JoinType,
    ) -> DaftResult<()> {
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("right.b", DataType::Utf8),
            Field::new("c", DataType::Float64),
        ]);
        let left_scan_plan = dummy_scan_node_with_pushdowns(
            left_scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_left_scan { None } else { Some(1) }),
        );
        let right_scan_plan = dummy_scan_node(right_scan_op.clone());
        let join_on = if null_equals_null {
            unresolved_col("b").eq_null_safe(unresolved_col("right.b"))
        } else {
            unresolved_col("b").eq(unresolved_col("right.b"))
        };

        let pred = resolved_col("a").lt(lit(2));
        let plan = left_scan_plan
            .join(
                &right_scan_plan,
                join_on.clone().into(),
                vec![],
                how,
                None,
                Default::default(),
            )?
            .filter(pred.clone())?
            .build();
        let expected_left_filter_scan = if push_into_left_scan {
            dummy_scan_node_with_pushdowns(
                left_scan_op.clone(),
                Pushdowns::default().with_filters(Some(pred)),
            )
        } else {
            left_scan_plan.filter(pred)?
        };
        let expected = expected_left_filter_scan
            .join(
                &right_scan_plan,
                join_on.into(),
                vec![],
                how,
                None,
                Default::default(),
            )?
            .build();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter can be pushed into the right side of a Join.
    #[rstest]
    fn filter_commutes_with_join_right_side(
        #[values(false, true)] push_into_right_scan: bool,
        #[values(false, true)] null_equals_null: bool,
        #[values(JoinType::Inner, JoinType::Right)] how: JoinType,
    ) -> DaftResult<()> {
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("right.b", DataType::Utf8),
            Field::new("c", DataType::Float64),
        ]);
        let left_scan_plan = dummy_scan_node(left_scan_op.clone());
        let right_scan_plan = dummy_scan_node_with_pushdowns(
            right_scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_right_scan { None } else { Some(1) }),
        );
        let join_on = if null_equals_null {
            unresolved_col("b").eq_null_safe(unresolved_col("right.b"))
        } else {
            unresolved_col("b").eq(unresolved_col("right.b"))
        };
        let pred = resolved_col("c").lt(lit(2.0));
        let plan = left_scan_plan
            .join(
                &right_scan_plan,
                join_on.clone().into(),
                vec![],
                how,
                None,
                Default::default(),
            )?
            .filter(pred.clone())?
            .build();
        let expected_right_filter_scan = if push_into_right_scan {
            dummy_scan_node_with_pushdowns(
                right_scan_op.clone(),
                Pushdowns::default().with_filters(Some(pred)),
            )
        } else {
            right_scan_plan.filter(pred)?
        };
        let expected = left_scan_plan
            .join(
                &expected_right_filter_scan,
                join_on.into(),
                vec![],
                how,
                None,
                Default::default(),
            )?
            .build();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter on join key commutes with Join.
    #[rstest]
    fn filter_commutes_with_join_on_join_key(
        #[values(false, true)] push_into_left_scan: bool,
        #[values(false, true)] push_into_right_scan: bool,
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Outer,
            JoinType::Anti,
            JoinType::Semi
        )]
        how: JoinType,
    ) -> DaftResult<()> {
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Utf8),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Float64),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("b", DataType::Int64),
            Field::new("d", DataType::Boolean),
        ]);
        let left_scan_plan = dummy_scan_node_with_pushdowns(
            left_scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_left_scan { None } else { Some(1) }),
        );
        let right_scan_plan = dummy_scan_node_with_pushdowns(
            right_scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_right_scan { None } else { Some(1) }),
        );
        let pred = resolved_col("b").is_null();
        let plan = left_scan_plan
            .join(
                &right_scan_plan,
                None,
                vec!["b".to_string()],
                how,
                None,
                Default::default(),
            )?
            .filter(pred.clone())?
            .build();
        let expected_left_filter_scan = if push_into_left_scan {
            dummy_scan_node_with_pushdowns(
                left_scan_op.clone(),
                Pushdowns::default().with_filters(Some(pred.clone())),
            )
        } else {
            left_scan_plan.filter(pred.clone())?
        };
        let expected_right_filter_scan = if push_into_right_scan {
            dummy_scan_node_with_pushdowns(
                right_scan_op,
                Pushdowns::default().with_filters(Some(pred)),
            )
        } else {
            right_scan_plan.filter(pred)?
        };
        let expected = expected_left_filter_scan
            .join(
                &expected_right_filter_scan,
                None,
                vec!["b".to_string()],
                how,
                None,
                Default::default(),
            )?
            .build();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter can be pushed into the left side of a Join.
    #[rstest]
    fn filter_does_not_commute_with_join_left_side(
        #[values(false, true)] null_equals_null: bool,
        #[values(JoinType::Right, JoinType::Outer)] how: JoinType,
    ) -> DaftResult<()> {
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("right.b", DataType::Utf8),
            Field::new("c", DataType::Float64),
        ]);
        let left_scan_plan = dummy_scan_node(left_scan_op.clone());
        let right_scan_plan = dummy_scan_node(right_scan_op.clone());
        let join_on = if null_equals_null {
            unresolved_col("b").eq_null_safe(unresolved_col("right.b"))
        } else {
            unresolved_col("b").eq(unresolved_col("right.b"))
        };
        let pred = resolved_col("a").is_null();
        let plan = left_scan_plan
            .join(
                &right_scan_plan,
                join_on.into(),
                vec![],
                how,
                None,
                Default::default(),
            )?
            .filter(pred)?
            .build();
        // should not push down filter
        let expected = plan.clone();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that Filter can be pushed into the right side of a Join.
    #[rstest]
    fn filter_does_not_commute_with_join_right_side(
        #[values(false, true)] null_equals_null: bool,
        #[values(JoinType::Left, JoinType::Outer)] how: JoinType,
    ) -> DaftResult<()> {
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("right.b", DataType::Utf8),
            Field::new("c", DataType::Float64),
        ]);
        let left_scan_plan = dummy_scan_node(left_scan_op.clone());
        let right_scan_plan = dummy_scan_node(right_scan_op.clone());
        let join_on = if null_equals_null {
            unresolved_col("b").eq_null_safe(unresolved_col("right.b"))
        } else {
            unresolved_col("b").eq(unresolved_col("right.b"))
        };

        let pred = resolved_col("c").is_null();
        let plan = left_scan_plan
            .join(
                &right_scan_plan,
                join_on.into(),
                vec![],
                how,
                None,
                Default::default(),
            )?
            .filter(pred)?
            .build();
        // should not push down filter
        let expected = plan.clone();
        assert_optimized_plan_eq(plan, expected, false)?;
        Ok(())
    }

    /// Tests that a complex predicate can be separated so that it can be pushed down into one side of the join.
    /// Modeled after TPC-H Q7
    #[rstest]
    fn filter_commutes_with_join_complex() -> DaftResult<()> {
        let left_scan_op = dummy_scan_operator(vec![Field::new("a", DataType::Utf8)]);
        let right_scan_op = dummy_scan_operator(vec![Field::new("b", DataType::Utf8)]);

        let plan = dummy_scan_node(left_scan_op.clone())
            .join(
                dummy_scan_node(right_scan_op.clone()),
                None,
                vec![],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .filter(
                (resolved_col("a")
                    .eq(lit("FRANCE"))
                    .and(resolved_col("b").eq(lit("GERMANY"))))
                .or(resolved_col("a")
                    .eq(lit("GERMANY"))
                    .and(resolved_col("b").eq(lit("FRANCE")))),
            )?
            .build();

        let expected = dummy_scan_node_with_pushdowns(
            left_scan_op,
            Pushdowns::default().with_filters(Some(
                resolved_col("a")
                    .eq(lit("FRANCE"))
                    .or(resolved_col("a").eq(lit("GERMANY"))),
            )),
        )
        .join(
            dummy_scan_node_with_pushdowns(
                right_scan_op,
                Pushdowns::default().with_filters(Some(
                    resolved_col("b")
                        .eq(lit("GERMANY"))
                        .or(resolved_col("b").eq(lit("FRANCE"))),
                )),
            ),
            None,
            vec![],
            JoinType::Inner,
            None,
            Default::default(),
        )?
        .filter(
            (resolved_col("a")
                .eq(lit("FRANCE"))
                .or(resolved_col("b").eq(lit("FRANCE"))))
            .and(
                resolved_col("b")
                    .eq(lit("GERMANY"))
                    .or(resolved_col("a").eq(lit("GERMANY"))),
            ),
        )?
        .build();

        assert_optimized_plan_eq(plan, expected, false)?;

        Ok(())
    }

    fn filter_pushdown_strict_mode_scenario(enable_strict_pushdown: bool) -> DaftResult<()> {
        let pred = resolved_col("value").is_in(vec![lit(2)]);
        let scan_op = dummy_scan_operator(vec![
            Field::new("date_col", DataType::Date),
            Field::new("value", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .filter(pred.clone())?
            .build();

        let expected = if enable_strict_pushdown {
            plan.clone()
        } else {
            dummy_scan_node_with_pushdowns(scan_op, Pushdowns::default().with_filters(Some(pred)))
                .build()
        };

        assert_optimized_plan_eq(plan, expected, enable_strict_pushdown)?;

        Ok(())
    }

    /// Tests that Filter pushdown respects the strict mode configuration.
    /// The main reason for not using rstest is that it seems unable to handle the setting of environment variables properly.
    #[test]
    fn filter_pushdown_strict_mode_true() -> DaftResult<()> {
        filter_pushdown_strict_mode_scenario(true)
    }

    #[test]
    fn filter_pushdown_strict_mode_false() -> DaftResult<()> {
        filter_pushdown_strict_mode_scenario(false)
    }
}
