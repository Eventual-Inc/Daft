use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::DaftResult;
use daft_dsl::{
    col,
    optimization::{
        conjuct, get_required_columns, replace_columns_with_expressions, split_conjuction,
    },
    ExprRef,
};
use daft_scan::{rewrite_predicate_for_partitioning, PredicateGroups};

use crate::{
    logical_ops::{Concat, Filter, Project, Source},
    source_info::SourceInfo,
    LogicalPlan,
};

use super::{ApplyOrder, OptimizerRule, Transformed};

/// Optimization rules for pushing Filters further into the logical plan.
#[derive(Default, Debug)]
pub struct PushDownFilter {}

impl PushDownFilter {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownFilter {
    fn apply_order(&self) -> ApplyOrder {
        ApplyOrder::TopDown
    }

    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let filter = match plan.as_ref() {
            LogicalPlan::Filter(filter) => filter,
            _ => return Ok(Transformed::No(plan)),
        };
        let child_plan = filter.input.as_ref();
        let new_plan = match child_plan {
            LogicalPlan::Filter(child_filter) => {
                // Combine filters.
                //
                // Filter-Filter --> Filter

                // Split predicate expression on conjunctions (ANDs).
                let parent_predicates = split_conjuction(&filter.predicate);
                let predicate_set: HashSet<&ExprRef> = parent_predicates.iter().cloned().collect();
                // Add child predicate expressions to parent predicate expressions, eliminating duplicates.
                let new_predicates: Vec<ExprRef> = parent_predicates
                    .iter()
                    .chain(
                        split_conjuction(&child_filter.predicate)
                            .iter()
                            .filter(|e| !predicate_set.contains(**e)),
                    )
                    .map(|e| (*e).clone())
                    .collect::<Vec<_>>();
                // Reconjunct predicate expressions.
                let new_predicate = conjuct(new_predicates).unwrap();
                let new_filter: Arc<LogicalPlan> =
                    LogicalPlan::from(Filter::try_new(child_filter.input.clone(), new_predicate)?)
                        .into();
                self.try_optimize(new_filter.clone())?
                    .or(Transformed::Yes(new_filter))
                    .unwrap()
                    .clone()
            }
            LogicalPlan::Source(source) => {
                match source.source_info.as_ref() {
                    // Filter pushdown is not supported for in-memory sources.
                    SourceInfo::InMemory(_) => return Ok(Transformed::No(plan)),
                    // Do not pushdown if Source node already has a limit
                    SourceInfo::Physical(external_info)
                        if let Some(_) = external_info.pushdowns.limit =>
                    {
                        return Ok(Transformed::No(plan))
                    }

                    // Pushdown filter into the Source node
                    SourceInfo::Physical(external_info) => {
                        let predicate = &filter.predicate;
                        let new_predicate = external_info
                            .pushdowns
                            .filters
                            .as_ref()
                            .map(|f| predicate.clone().and(f.clone()))
                            .unwrap_or(predicate.clone());
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
                            external_info.scan_op.0.partitioning_keys(),
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
                            return Ok(Transformed::No(plan));
                        }

                        let data_filter = conjuct(data_only_filter);
                        let partition_filter = conjuct(partition_only_filter);
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
                                conjuct(needing_filter_op).unwrap(),
                            )?
                            .into();
                            return Ok(Transformed::Yes(filter_op.into()));
                        } else {
                            return Ok(Transformed::Yes(new_source.into()));
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
                let predicates = split_conjuction(&filter.predicate);
                let projection_input_mapping = child_project
                    .projection
                    .iter()
                    .filter_map(|e| e.input_mapping().map(|s| (e.name().to_string(), col(s))))
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
                    let predicate_cols = get_required_columns(predicate);
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
                    return Ok(Transformed::No(plan));
                }
                // Create new Filter with predicates that can be pushed past Projection.
                let predicates_to_push = conjuct(can_push).unwrap();
                let push_down_filter: LogicalPlan =
                    Filter::try_new(child_project.input.clone(), predicates_to_push)?.into();
                // Create new Projection.
                let new_projection: LogicalPlan = Project::try_new(
                    push_down_filter.into(),
                    child_project.projection.clone(),
                    child_project.resource_request.clone(),
                )?
                .into();
                if can_not_push.is_empty() {
                    // If all Filter predicate expressions were pushable past Projection, return new
                    // Projection-Filter subplan.
                    new_projection.into()
                } else {
                    // Otherwise, add a Filter after Projection that filters with predicate expressions
                    // that couldn't be pushed past the Projection, returning a Filter-Projection-Filter subplan.
                    let post_projection_predicate = conjuct(can_not_push).unwrap();
                    let post_projection_filter: LogicalPlan =
                        Filter::try_new(new_projection.into(), post_projection_predicate)?.into();
                    post_projection_filter.into()
                }
            }
            LogicalPlan::Sort(_) | LogicalPlan::Repartition(_) => {
                // Naive commuting with unary ops.
                let new_filter = plan
                    .with_new_children(&[child_plan.children()[0].clone()])
                    .into();
                child_plan.with_new_children(&[new_filter]).into()
            }
            LogicalPlan::Concat(Concat { input, other }) => {
                // Push filter into each side of the concat.
                let new_input: LogicalPlan =
                    Filter::try_new(input.clone(), filter.predicate.clone())?.into();
                let new_other: LogicalPlan =
                    Filter::try_new(other.clone(), filter.predicate.clone())?.into();
                let new_concat: LogicalPlan =
                    Concat::try_new(new_input.into(), new_other.into())?.into();
                new_concat.into()
            }
            LogicalPlan::Join(child_join) => {
                // Push filter into each side of the join.
                // TODO(Clark): Merge filter predicate with on predicate, if present.
                // TODO(Clark): Duplicate filters for joined columns so filters can be pushed down to both sides.

                // Get all input columns for predicate.
                let predicate_cols: HashSet<_> = get_required_columns(&filter.predicate)
                    .iter()
                    .cloned()
                    .collect();
                // Only push the filter into the left side of the join if the left side of the join has all columns
                // required by the predicate.
                let left_cols: HashSet<_> =
                    child_join.left.schema().names().iter().cloned().collect();
                let can_push_left = left_cols
                    .intersection(&predicate_cols)
                    .collect::<HashSet<_>>()
                    .len()
                    == predicate_cols.len();
                // Only push the filter into the right side of the join if the right side of the join has all columns
                // required by the predicate.
                let right_cols: HashSet<_> =
                    child_join.right.schema().names().iter().cloned().collect();
                let can_push_right = right_cols
                    .intersection(&predicate_cols)
                    .collect::<HashSet<_>>()
                    .len()
                    == predicate_cols.len();
                if !can_push_left && !can_push_right {
                    return Ok(Transformed::No(plan));
                }
                let new_left: Arc<LogicalPlan> = if can_push_left {
                    LogicalPlan::from(Filter::try_new(
                        child_join.left.clone(),
                        filter.predicate.clone(),
                    )?)
                    .into()
                } else {
                    child_join.left.clone()
                };
                let new_right: Arc<LogicalPlan> = if can_push_right {
                    LogicalPlan::from(Filter::try_new(
                        child_join.right.clone(),
                        filter.predicate.clone(),
                    )?)
                    .into()
                } else {
                    child_join.right.clone()
                };
                child_plan.with_new_children(&[new_left, new_right]).into()
            }
            _ => return Ok(Transformed::No(plan)),
        };
        Ok(Transformed::Yes(new_plan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::{datatypes::Field, join::JoinType, DataType};
    use daft_dsl::{col, lit};
    use daft_scan::Pushdowns;
    use rstest::rstest;

    use crate::{
        logical_optimization::{rules::PushDownFilter, test::assert_optimized_plan_with_rules_eq},
        test::{dummy_scan_node, dummy_scan_node_with_pushdowns, dummy_scan_operator},
        LogicalPlan,
    };

    /// Helper that creates an optimizer with the PushDownFilter rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(plan, expected, vec![Box::new(PushDownFilter::new())])
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
                .filter(col("a").lt(lit(2)))?
                .build();
        // Plan should be unchanged after optimization.
        let expected = plan.clone();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests combining of two Filters by merging their predicates.
    #[rstest]
    fn filter_combine_with_filter(#[values(false, true)] push_into_scan: bool) -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_scan { None } else { Some(1) }),
        );
        let p1 = col("a").lt(lit(2));
        let p2 = col("b").eq(lit("foo"));
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
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that we can't pushdown a filter into a ScanOperator if it has an udf-ish expression.
    #[test]
    fn filter_with_udf_not_pushed_down_into_scan() -> DaftResult<()> {
        let pred = daft_functions::uri::download(col("a"), 1, true, true, None);
        let plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]))
        .filter(pred.is_null())?
        .build();
        let expected = plan.clone();
        assert_optimized_plan_eq(plan, expected)?;
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
        let pred = col("a").lt(lit(2));
        let proj = vec![col("a")];
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
        assert_optimized_plan_eq(plan, expected)?;
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
        let pred = col("a").lt(lit(2)).and(col("b").eq(lit("foo")));
        let proj = vec![col("a"), col("b")];
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
        assert_optimized_plan_eq(plan, expected)?;
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
        .select(vec![col("a").add(lit(1))])?
        .filter(col("a").lt(lit(2)))?
        .build();
        // Filter should NOT commute with Project, since this would involve redundant computation.
        let expected = plan.clone();
        assert_optimized_plan_eq(plan, expected)?;
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
        let pred = col("a").lt(lit(2));
        let proj = vec![col("a").add(lit(1))];
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
        assert_optimized_plan_eq(plan, expected)?;
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
        let pred = col("a").lt(lit(2));
        let sort_by = vec![col("a")];
        let descending = vec![true];
        let plan = scan_plan
            .sort(sort_by.clone(), descending.clone())?
            .filter(pred.clone())?
            .build();
        let expected_filter_scan = if push_into_scan {
            dummy_scan_node_with_pushdowns(scan_op, Pushdowns::default().with_filters(Some(pred)))
        } else {
            scan_plan.filter(pred)?
        };
        let expected = expected_filter_scan.sort(sort_by, descending)?.build();
        assert_optimized_plan_eq(plan, expected)?;
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
        let pred = col("a").lt(lit(2));
        let num_partitions = 1;
        let repartition_by = vec![col("a")];
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
        assert_optimized_plan_eq(plan, expected)?;
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
        let pred = col("a").lt(lit(2));
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
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Filter commutes with Join.
    #[rstest]
    fn filter_commutes_with_join(
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
        let join_on = vec![col("b")];
        let pred = col("a").lt(lit(2));
        let plan = left_scan_plan
            .join(
                &right_scan_plan,
                join_on.clone(),
                join_on.clone(),
                JoinType::Inner,
                None,
            )?
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
            .join(
                &expected_right_filter_scan,
                join_on.clone(),
                join_on.clone(),
                JoinType::Inner,
                None,
            )?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Filter can be pushed into the left side of a Join.
    #[rstest]
    fn filter_commutes_with_join_left_side(
        #[values(false, true)] push_into_left_scan: bool,
    ) -> DaftResult<()> {
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("b", DataType::Utf8),
            Field::new("c", DataType::Float64),
        ]);
        let left_scan_plan = dummy_scan_node_with_pushdowns(
            left_scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_left_scan { None } else { Some(1) }),
        );
        let right_scan_plan = dummy_scan_node(right_scan_op.clone());
        let join_on = vec![col("b")];
        let pred = col("a").lt(lit(2));
        let plan = left_scan_plan
            .join(
                &right_scan_plan,
                join_on.clone(),
                join_on.clone(),
                JoinType::Inner,
                None,
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
        let expected = expected_left_filter_scan
            .join(
                &right_scan_plan,
                join_on.clone(),
                join_on.clone(),
                JoinType::Inner,
                None,
            )?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Filter can be pushed into the right side of a Join.
    #[rstest]
    fn filter_commutes_with_join_right_side(
        #[values(false, true)] push_into_right_scan: bool,
    ) -> DaftResult<()> {
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("b", DataType::Utf8),
            Field::new("c", DataType::Float64),
        ]);
        let left_scan_plan = dummy_scan_node(left_scan_op.clone());
        let right_scan_plan = dummy_scan_node_with_pushdowns(
            right_scan_op.clone(),
            Pushdowns::default().with_limit(if push_into_right_scan { None } else { Some(1) }),
        );
        let join_on = vec![col("b")];
        let pred = col("c").lt(lit(2.0));
        let plan = left_scan_plan
            .join(
                &right_scan_plan,
                join_on.clone(),
                join_on.clone(),
                JoinType::Inner,
                None,
            )?
            .filter(pred.clone())?
            .build();
        let expected_right_filter_scan = if push_into_right_scan {
            dummy_scan_node_with_pushdowns(
                right_scan_op.clone(),
                Pushdowns::default().with_filters(Some(pred.clone())),
            )
        } else {
            right_scan_plan.filter(pred.clone())?
        };
        let expected = left_scan_plan
            .join(
                &expected_right_filter_scan,
                join_on.clone(),
                join_on.clone(),
                JoinType::Inner,
                None,
            )?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Filter on join key commutes with Join.
    #[rstest]
    fn filter_commutes_with_join_on_join_key(
        #[values(false, true)] push_into_left_scan: bool,
        #[values(false, true)] push_into_right_scan: bool,
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
        let join_on = vec![col("b")];
        let pred = col("b").lt(lit(2));
        let plan = left_scan_plan
            .join(
                &right_scan_plan,
                join_on.clone(),
                join_on.clone(),
                JoinType::Inner,
                None,
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
                join_on.clone(),
                join_on.clone(),
                JoinType::Inner,
                None,
            )?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
