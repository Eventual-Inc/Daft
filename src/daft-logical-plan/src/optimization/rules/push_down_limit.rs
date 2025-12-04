use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{DynTreeNode, Transformed, TreeNode};
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
                    LogicalPlan::Repartition(_) | LogicalPlan::IntoBatches(_) => {
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
                    LogicalPlan::Source(LogicalSource {
                        output_schema,
                        source_info,
                        ..
                    }) => {
                        let pushdown_limit = limit + offset.unwrap_or(0) as usize;
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
                                let new_source = LogicalPlan::Source(LogicalSource::new(
                                    output_schema.clone(),
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
                                let new_source = LogicalPlan::Source(LogicalSource::new(
                                    output_schema.clone(),
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
                    LogicalPlan::Join(_)
                    | LogicalPlan::Filter(_)
                    | LogicalPlan::Distinct(_)
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
                    | LogicalPlan::Concat(_)
                    | LogicalPlan::VLLMProject(..) => Ok(Transformed::no(plan)),
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
    use common_scan_info::Pushdowns;
    use daft_core::prelude::*;
    use daft_dsl::unresolved_col;
    #[cfg(feature = "python")]
    use pyo3::Python;
    use rstest::rstest;

    use crate::{
        LogicalPlan, LogicalPlanBuilder,
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::PushDownLimit,
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
}
