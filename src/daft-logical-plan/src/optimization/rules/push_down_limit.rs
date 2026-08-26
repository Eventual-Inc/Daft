use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{DynTreeNode, Transformed, TreeNode};
use daft_core::join::JoinType;
use daft_dsl::{Expr, ExprRef, functions::scalar::ScalarFn};

use super::OptimizerRule;
use crate::{
    LogicalPlan,
    ops::{
        Limit as LogicalLimit, Project as LogicalProject, Sort as LogicalSort,
        Source as LogicalSource, TopN as LogicalTopN,
    },
    source_info::SourceInfo,
};

/// Optimization rules for pushing Limits further into the logical plan.
#[derive(Default, Debug)]
pub struct PushDownLimit {}

impl PushDownLimit {
    pub fn new() -> Self {
        Self {}
    }

    fn contains_explode(expr: &ExprRef) -> bool {
        expr.exists(|e| {
            if let Expr::ScalarFn(ScalarFn::Builtin(sf)) = e.as_ref() {
                sf.is_function_type::<daft_functions_list::Explode>()
            } else {
                false
            }
        })
    }
}

impl OptimizerRule for PushDownLimit {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| self.try_optimize_node(node))
    }
}

impl PushDownLimit {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Limit(LogicalLimit {
                input,
                limit,
                offset,
                eager,
                ..
            }) => {
                let limit = *limit as usize;
                match input.as_ref() {
                    // Naive commuting with unary ops.
                    //
                    // Limit-UnaryOp -> UnaryOp-Limit
                    LogicalPlan::Repartition(_)
                    | LogicalPlan::IntoBatches(_)
                    | LogicalPlan::IntoPartitions(_) => {
                        let new_limit = plan
                            .with_new_children(&[input.arc_children()[0].clone()])
                            .into();
                        Ok(Transformed::yes(
                            input.with_new_children(&[new_limit]).into(),
                        ))
                    }
                    LogicalPlan::Project(LogicalProject { projection, .. }) => {
                        let has_explode = projection.iter().any(Self::contains_explode);

                        if has_explode {
                            Ok(Transformed::no(plan))
                        } else {
                            let new_limit = plan
                                .with_new_children(&[input.arc_children()[0].clone()])
                                .into();
                            Ok(Transformed::yes(
                                input.with_new_children(&[new_limit]).into(),
                            ))
                        }
                    }
                    // Push limit into source as a "local" limit.
                    //
                    // Limit-Source -> Limit-Source[with_limit]
                    LogicalPlan::Source(source) => {
                        let LogicalSource { source_info, .. } = source;
                        // Do the arithmetic in u64 so a large offset cannot overflow.
                        let pushdown_limit: usize = (limit as u64)
                            .saturating_add(offset.unwrap_or(0))
                            .try_into()
                            .unwrap_or(usize::MAX);
                        match source_info.as_ref() {
                            // Limit pushdown is not supported for in-memory sources.
                            SourceInfo::InMemory(_) => Ok(Transformed::no(plan)),
                            SourceInfo::GlobScan(glob_info)
                                if let Some(existing_limit) = glob_info.pushdowns.limit
                                    && existing_limit <= pushdown_limit =>
                            {
                                Ok(Transformed::no(plan))
                            }
                            SourceInfo::GlobScan(glob_info) => {
                                // Create a new GlobScanSource node with the new limit
                                let new_pushdowns =
                                    glob_info.pushdowns.with_limit(Some(pushdown_limit));
                                let new_glob_info = glob_info.with_pushdowns(new_pushdowns);
                                let new_source =
                                    LogicalPlan::Source(source.clone().with_source_info(
                                        SourceInfo::GlobScan(new_glob_info).into(),
                                    ))
                                    .into();
                                // Set the GlobScanSource node as the child of the Limit node
                                let limit_plan = plan.with_new_children(&[new_source]).into();
                                Ok(Transformed::yes(limit_plan))
                            }
                            // Do not pushdown if Source node is already more limited than `limit`
                            SourceInfo::Physical(external_info)
                                if let Some(existing_limit) = external_info.pushdowns.limit
                                    && existing_limit <= pushdown_limit =>
                            {
                                Ok(Transformed::no(plan))
                            }
                            // Pushdown limit into the Source node as a "local" limit
                            SourceInfo::Physical(external_info) => {
                                let new_pushdowns =
                                    external_info.pushdowns.with_limit(Some(pushdown_limit));
                                let new_external_info = external_info.with_pushdowns(new_pushdowns);
                                let new_source =
                                    LogicalPlan::Source(source.clone().with_source_info(
                                        SourceInfo::Physical(new_external_info).into(),
                                    ))
                                    .into();
                                let out_plan =
                                    if external_info.scan_state.get_scan_op().0.can_absorb_limit()
                                        && offset.is_none()
                                    {
                                        new_source
                                    } else {
                                        plan.with_new_children(&[new_source]).into()
                                    };
                                Ok(Transformed::yes(out_plan))
                            }
                            SourceInfo::PlaceHolder(..) => {
                                panic!("PlaceHolderInfo should not exist for optimization!");
                            }
                        }
                    }
                    // Fold Limit together.
                    //
                    // Limit-Limit -> Limit
                    LogicalPlan::Limit(LogicalLimit {
                        input,
                        offset: child_offset,
                        limit: child_limit,
                        eager: child_eager,
                        ..
                    }) => {
                        let new_offset = match (*offset, *child_offset) {
                            (Some(o1), Some(o2)) => Some(o1 + o2),
                            (Some(o1), None) => Some(o1),
                            (None, Some(o2)) => Some(o2),
                            (None, None) => None,
                        };

                        let new_limit =
                            (limit as u64).min(child_limit.saturating_sub(offset.unwrap_or(0)));
                        let new_eager = eager | child_eager;
                        let new_plan = Arc::new(LogicalPlan::Limit(LogicalLimit::new(
                            input.clone(),
                            new_limit,
                            new_offset,
                            new_eager,
                        )));
                        // we rerun the optimizer, ideally when we move to a visitor pattern this should go away
                        let optimized = self
                            .try_optimize_node(new_plan.clone())?
                            .or(Transformed::yes(new_plan))
                            .data;
                        Ok(Transformed::yes(optimized))
                    }
                    // Combine Limit with Sort into TopN
                    //
                    // Limit-Sort -> TopN
                    LogicalPlan::Sort(LogicalSort {
                        input,
                        sort_by,
                        descending,
                        nulls_first,
                        ..
                    }) => {
                        let new_plan = Arc::new(LogicalPlan::TopN(LogicalTopN::try_new(
                            input.clone(),
                            sort_by.clone(),
                            descending.clone(),
                            nulls_first.clone(),
                            limit as u64,
                            *offset,
                        )?));

                        Ok(Transformed::yes(new_plan))
                    }
                    // Push limit onto the outer side of a Left/Right join.
                    //
                    // Limit-LeftJoin(l, r) -> Limit-LeftJoin(Limit-l, r)
                    //
                    // Every output row of a Left join is attributable to exactly one
                    // left row, and the rows a given left row contributes depend only
                    // on that row and the (untouched) right input. Limiting the left
                    // input therefore only drops whole groups of output rows, and the
                    // retained outer Limit performs the final trim. The Right join case
                    // is symmetric.
                    //
                    // Outer joins must not be pushed into on either side: dropping rows
                    // from one input turns rows of the other input that used to match
                    // into unmatched rows, so the join emits null-extended rows that the
                    // original plan never produces. Inner/Anti/Semi joins can drop rows
                    // of either input, so neither side is row-preserving.
                    LogicalPlan::Join(join) => {
                        let push_left = match join.join_type {
                            JoinType::Left => true,
                            JoinType::Right => false,
                            JoinType::Inner | JoinType::Outer | JoinType::Anti | JoinType::Semi => {
                                return Ok(Transformed::no(plan));
                            }
                        };
                        // The number of rows that could be relevant to the output is
                        // limit + offset (rows that are skipped still need to exist).
                        // Compute in u64 so that a large offset cannot overflow.
                        let pushdown_limit = (limit as u64).saturating_add(offset.unwrap_or(0));
                        // Wrap `child` in a Limit unless it already emits an
                        // equal-or-smaller number of rows (guards against re-pushing on
                        // subsequent optimizer passes). A child Limit emits at most
                        // `limit` rows regardless of its offset, so the offset is
                        // irrelevant here. Look through the operators that this rule
                        // commutes with, because a Limit pushed down by an earlier pass
                        // may have since commuted below them.
                        let make_limited = |child: &Arc<LogicalPlan>| -> Option<Arc<LogicalPlan>> {
                            let mut node = child.as_ref();
                            loop {
                                node = match node {
                                    LogicalPlan::Project(LogicalProject {
                                        input,
                                        projection,
                                        ..
                                    }) if !projection.iter().any(Self::contains_explode) => {
                                        input.as_ref()
                                    }
                                    LogicalPlan::Repartition(repartition) => {
                                        repartition.input.as_ref()
                                    }
                                    LogicalPlan::IntoBatches(into_batches) => {
                                        into_batches.input.as_ref()
                                    }
                                    LogicalPlan::IntoPartitions(into_partitions) => {
                                        into_partitions.input.as_ref()
                                    }
                                    _ => break,
                                };
                            }
                            if let LogicalPlan::Limit(LogicalLimit {
                                limit: child_limit, ..
                            }) = node
                                && *child_limit <= pushdown_limit
                            {
                                return None;
                            }
                            Some(Arc::new(LogicalPlan::Limit(LogicalLimit::new(
                                child.clone(),
                                pushdown_limit,
                                None,
                                *eager,
                            ))))
                        };
                        let (outer_side, other_side) = if push_left {
                            (&join.left, &join.right)
                        } else {
                            (&join.right, &join.left)
                        };
                        let Some(new_outer_side) = make_limited(outer_side) else {
                            return Ok(Transformed::no(plan));
                        };
                        let new_children = if push_left {
                            [new_outer_side, other_side.clone()]
                        } else {
                            [other_side.clone(), new_outer_side]
                        };
                        let new_join = input.with_new_children(&new_children).into();
                        Ok(Transformed::yes(plan.with_new_children(&[new_join]).into()))
                    }
                    LogicalPlan::AsofJoin(_)
                    | LogicalPlan::Filter(_)
                    | LogicalPlan::Distinct(_)
                    | LogicalPlan::Offset(_)
                    | LogicalPlan::TopN(..)
                    | LogicalPlan::Sample(..)
                    | LogicalPlan::Shuffle(..)
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
                    | LogicalPlan::Concat(_)
                    | LogicalPlan::VLLMProject(..)
                    | LogicalPlan::StageCheckpointKeys(..) => Ok(Transformed::no(plan)),
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_dsl::unresolved_col;
    use daft_scan::Pushdowns;
    #[cfg(feature = "python")]
    use pyo3::Python;
    use rstest::rstest;

    use crate::{
        LogicalPlan, LogicalPlanBuilder,
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::{OptimizerRule, PushDownLimit},
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_node_with_pushdowns, dummy_scan_operator},
    };

    /// Helper that creates an optimizer with the PushDownLimit rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(PushDownLimit::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    /// Tests that Limit pushes into external Source.
    ///
    /// Limit-Source -> Source[with_limit]
    #[test]
    fn limit_pushes_into_external_source() -> DaftResult<()> {
        let limit = 5;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(limit as usize)),
        )
        .limit(limit, false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit does not push into scan with existing smaller limit.
    ///
    /// Limit-Source[existing_limit] -> Source[existing_limit]
    #[test]
    fn limit_does_not_push_into_scan_if_smaller_limit() -> DaftResult<()> {
        let limit = 5;
        let existing_limit = 3;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(Some(existing_limit)),
        )
        .limit(limit, false)?
        .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(existing_limit)),
        )
        .limit(limit, false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit does push into scan with existing larger limit.
    ///
    /// Limit-Source[existing_limit] -> Source[new_limit]
    #[test]
    fn limit_does_push_into_scan_if_larger_limit() -> DaftResult<()> {
        let limit = 5;
        let existing_limit = 10;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(Some(existing_limit)),
        )
        .limit(limit, false)?
        .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(limit as usize)),
        )
        .limit(limit, false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that multiple adjacent Limits with offset fold into the smallest limit.
    ///
    /// Limit[o1, l1]-Limit[o2, l2] -> Limit[o1 + o2, min(l1,l2)]
    #[rstest]
    fn limit_folds_with_smaller_limit(
        #[values(false, true)] none_offset: bool,
        #[values(false, true)] smaller_first: bool,
    ) -> DaftResult<()> {
        let smaller_limit = 5;
        let limit = 10;
        let offset = if none_offset { None } else { Some(1) };
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .limit_with_offset(
                if smaller_first { smaller_limit } else { limit },
                offset,
                false,
            )?
            .limit(if smaller_first { limit } else { smaller_limit }, false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some((offset.unwrap_or(0) + smaller_limit) as usize)),
        )
        .limit_with_offset(smaller_limit, offset, false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit does not push into in-memory Source.
    #[test]
    #[cfg(feature = "python")]
    fn limit_does_not_push_into_in_memory_source() -> DaftResult<()> {
        let py_obj = Python::attach(|py| py.None());
        let schema: Arc<Schema> = Schema::new(vec![Field::new("a", DataType::Int64)]).into();
        let plan = LogicalPlanBuilder::in_memory_scan(
            "foo",
            common_partitioning::PartitionCacheEntry::Python(Arc::new(py_obj)),
            schema,
            Default::default(),
            5,
            3,
        )?
        .limit(5, false)?
        .build();
        assert_optimized_plan_eq(plan.clone(), plan)?;
        Ok(())
    }

    /// Tests that Limit commutes with Repartition.
    ///
    /// Limit-Repartition-Source -> Repartition-Source[with_limit]
    #[test]
    fn limit_commutes_with_repartition() -> DaftResult<()> {
        let limit = 5;
        let num_partitions = 1;
        let partition_by = vec![unresolved_col("a")];
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .hash_repartition(Some(num_partitions), partition_by.clone())?
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(limit as usize)),
        )
        .limit(limit, false)?
        .hash_repartition(Some(num_partitions), partition_by)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit commutes with Projections.
    ///
    /// Limit-Project-Source -> Project-Source[with_limit]
    #[test]
    fn limit_commutes_with_projection() -> DaftResult<()> {
        let limit = 5;
        let proj = vec![unresolved_col("a")];
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .select(proj.clone())?
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(limit as usize)),
        )
        .limit(limit, false)?
        .select(proj)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit commutes with IntoBatches.
    ///
    /// Limit-IntoBatches-Source -> IntoBatches-Source[with_limit]
    #[test]
    fn limit_commutes_with_into_batches() -> DaftResult<()> {
        let limit = 5;
        let batch_size = 10usize;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .into_batches(batch_size)?
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(limit as usize)),
        )
        .limit(limit, false)?
        .into_batches(batch_size)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit pushes onto the (preserved) left side of a Left join.
    ///
    /// Limit-LeftJoin(l, r) -> Limit-LeftJoin(Limit-l, r)
    #[test]
    fn limit_pushes_into_left_side_of_left_join() -> DaftResult<()> {
        let limit = 5;
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("c", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(left_scan_op.clone())
            .join(
                dummy_scan_node(right_scan_op.clone()),
                None,
                vec!["a".to_string()],
                JoinType::Left,
                None,
                Default::default(),
            )?
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            left_scan_op,
            Pushdowns::default().with_limit(Some(limit as usize)),
        )
        .limit(limit, false)?
        .join(
            dummy_scan_node(right_scan_op),
            None,
            vec!["a".to_string()],
            JoinType::Left,
            None,
            Default::default(),
        )?
        .limit(limit, false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit pushes onto the (preserved) right side of a Right join.
    ///
    /// Limit-RightJoin(l, r) -> Limit-RightJoin(l, Limit-r)
    #[test]
    fn limit_pushes_into_right_side_of_right_join() -> DaftResult<()> {
        let limit = 5;
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("c", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(left_scan_op.clone())
            .join(
                dummy_scan_node(right_scan_op.clone()),
                None,
                vec!["a".to_string()],
                JoinType::Right,
                None,
                Default::default(),
            )?
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node(left_scan_op)
            .join(
                dummy_scan_node_with_pushdowns(
                    right_scan_op,
                    Pushdowns::default().with_limit(Some(limit as usize)),
                )
                .limit(limit, false)?,
                None,
                vec!["a".to_string()],
                JoinType::Right,
                None,
                Default::default(),
            )?
            .limit(limit, false)?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit with an offset pushes `limit + offset` onto the outer
    /// side while retaining the original offset on the outer Limit.
    ///
    /// Limit[l, o]-LeftJoin(l, r) -> Limit[l, o]-LeftJoin(Limit[l+o]-l, r)
    #[test]
    fn limit_with_offset_pushes_limit_plus_offset_through_left_join() -> DaftResult<()> {
        let limit = 5;
        let offset = 3;
        let pushed = limit + offset;
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("c", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(left_scan_op.clone())
            .join(
                dummy_scan_node(right_scan_op.clone()),
                None,
                vec!["a".to_string()],
                JoinType::Left,
                None,
                Default::default(),
            )?
            .limit_with_offset(limit, Some(offset), false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            left_scan_op,
            Pushdowns::default().with_limit(Some(pushed as usize)),
        )
        .limit(pushed, false)?
        .join(
            dummy_scan_node(right_scan_op),
            None,
            vec!["a".to_string()],
            JoinType::Left,
            None,
            Default::default(),
        )?
        .limit_with_offset(limit, Some(offset), false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit does not re-wrap an outer side that is already limited
    /// to an equal-or-smaller number of rows (idempotency guard).
    #[test]
    fn limit_does_not_rewrap_already_smaller_limit_on_left_join() -> DaftResult<()> {
        let limit = 5;
        let existing_limit = 2;
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("c", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(left_scan_op.clone())
            .limit(existing_limit, false)?
            .join(
                dummy_scan_node(right_scan_op.clone()),
                None,
                vec!["a".to_string()],
                JoinType::Left,
                None,
                Default::default(),
            )?
            .limit(limit, false)?
            .build();
        // The existing (smaller) limit is preserved and merely pushed into the
        // source; no additional Limit is added around the left input.
        let expected = dummy_scan_node_with_pushdowns(
            left_scan_op,
            Pushdowns::default().with_limit(Some(existing_limit as usize)),
        )
        .limit(existing_limit, false)?
        .join(
            dummy_scan_node(right_scan_op),
            None,
            vec!["a".to_string()],
            JoinType::Left,
            None,
            Default::default(),
        )?
        .limit(limit, false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that an already-limited outer side is not re-wrapped even when its
    /// Limit carries an offset, which would otherwise make the rule oscillate:
    /// the added Limit gets folded back into the child on the next pass.
    #[test]
    fn limit_does_not_rewrap_already_smaller_limit_with_offset_on_left_join() -> DaftResult<()> {
        let limit = 5;
        let existing_limit = 2;
        let existing_offset = 1;
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("c", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(left_scan_op.clone())
            .limit_with_offset(existing_limit, Some(existing_offset), false)?
            .join(
                dummy_scan_node(right_scan_op.clone()),
                None,
                vec!["a".to_string()],
                JoinType::Left,
                None,
                Default::default(),
            )?
            .limit(limit, false)?
            .build();
        // A Limit emits at most `existing_limit` rows regardless of its offset, so
        // no additional Limit is added around the left input; only the child Limit
        // pushes `existing_limit + existing_offset` into the source.
        let expected = dummy_scan_node_with_pushdowns(
            left_scan_op,
            Pushdowns::default().with_limit(Some((existing_limit + existing_offset) as usize)),
        )
        .limit_with_offset(existing_limit, Some(existing_offset), false)?
        .join(
            dummy_scan_node(right_scan_op),
            None,
            vec!["a".to_string()],
            JoinType::Left,
            None,
            Default::default(),
        )?
        .limit(limit, false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit does not push through joins that have no outer side
    /// (Inner/Anti/Semi), nor through Outer joins, where limiting one input would
    /// turn matched rows of the other input into null-extended rows.
    #[rstest]
    #[case(JoinType::Inner)]
    #[case(JoinType::Outer)]
    #[case(JoinType::Anti)]
    #[case(JoinType::Semi)]
    fn limit_does_not_push_through_join_without_outer_side(
        #[case] join_type: JoinType,
    ) -> DaftResult<()> {
        let limit = 5;
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("c", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(left_scan_op)
            .join(
                dummy_scan_node(right_scan_op),
                None,
                vec!["a".to_string()],
                join_type,
                None,
                Default::default(),
            )?
            .limit(limit, false)?
            .build();
        assert_optimized_plan_eq(plan.clone(), plan)?;
        Ok(())
    }

    /// Tests that the join-side idempotency guard looks through the operators
    /// that this rule commutes with. After an earlier pass pushes a Limit onto
    /// the outer side, that Limit commutes below the Project/Repartition/
    /// IntoBatches/IntoPartitions node, and the rule must still recognize it
    /// and reach a fixed point instead of re-wrapping on every pass.
    #[rstest]
    #[case::project("project")]
    #[case::repartition("repartition")]
    #[case::into_batches("into_batches")]
    #[case::into_partitions("into_partitions")]
    fn limit_does_not_rewrap_smaller_limit_below_commuting_op_on_left_join(
        #[case] commuting_op: &str,
    ) -> DaftResult<()> {
        let limit = 5;
        let existing_limit = 2;
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("c", DataType::Utf8),
        ]);
        // The outer side as left behind by an earlier pass: a small Limit
        // (already pushed into the source) sitting below a commuting operator.
        let left_side = dummy_scan_node_with_pushdowns(
            left_scan_op,
            Pushdowns::default().with_limit(Some(existing_limit as usize)),
        )
        .limit(existing_limit, false)?;
        let left_side = match commuting_op {
            "project" => left_side.select(vec![unresolved_col("a")])?,
            "repartition" => left_side.hash_repartition(Some(1), vec![unresolved_col("a")])?,
            "into_batches" => left_side.into_batches(10)?,
            _ => left_side.into_partitions(2)?,
        };
        let plan = left_side
            .join(
                dummy_scan_node(right_scan_op),
                None,
                vec!["a".to_string()],
                JoinType::Left,
                None,
                Default::default(),
            )?
            .limit(limit, false)?
            .build();
        // The rule must be a fixed point on this plan: re-wrapping the outer
        // side would ping-pong on every optimizer pass.
        let result = PushDownLimit::new().try_optimize(plan.clone())?;
        assert!(!result.transformed);
        assert_eq!(result.data, plan);
        Ok(())
    }

    /// Tests that `limit + offset` is computed in u64 when pushed through a
    /// join, so a huge offset saturates instead of overflowing usize (which
    /// would panic in debug builds).
    #[test]
    fn limit_plus_offset_does_not_overflow_through_left_join() -> DaftResult<()> {
        let limit = u64::MAX;
        let offset = 1;
        let left_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let right_scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("c", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(left_scan_op.clone())
            .join(
                dummy_scan_node(right_scan_op.clone()),
                None,
                vec!["a".to_string()],
                JoinType::Left,
                None,
                Default::default(),
            )?
            .limit_with_offset(limit, Some(offset), false)?
            .build();
        // limit + offset saturates at u64::MAX, which is usize::MAX on 64-bit
        // platforms.
        let expected = dummy_scan_node_with_pushdowns(
            left_scan_op,
            Pushdowns::default().with_limit(Some(usize::MAX)),
        )
        .limit(u64::MAX, false)?
        .join(
            dummy_scan_node(right_scan_op),
            None,
            vec!["a".to_string()],
            JoinType::Left,
            None,
            Default::default(),
        )?
        .limit_with_offset(limit, Some(offset), false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that `limit + offset` is computed in u64 when pushed into a
    /// source, so a huge offset saturates instead of overflowing usize.
    #[test]
    fn limit_plus_offset_does_not_overflow_into_source() -> DaftResult<()> {
        let limit = u64::MAX;
        let offset = 1;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .limit_with_offset(limit, Some(offset), false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(usize::MAX)),
        )
        .limit_with_offset(limit, Some(offset), false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
