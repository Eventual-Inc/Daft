use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{lit, resolved_col};

use crate::{
    ops::{
        Filter as LogicalFilter, MonotonicallyIncreasingId as RowNumber, Offset as LogicalOffset,
        Project as LogicalProject,
    },
    optimization::rules::OptimizerRule,
    LogicalPlan,
};

/// This rule is used to converting `offset` to `project + filter + row_number` to implement offset
/// semantics. Transforms example:
/// ```text
/// - Offset (row count = x)
///    - ...
/// ```
///
/// Into:
/// ```text
/// - Project (prune rowNumber symbol)
///    - Filter (rowNumber > x)
///       - RowNumber
///          - ...
/// ```
#[derive(Default, Debug)]
pub struct RewriteOffset {}

impl RewriteOffset {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for RewriteOffset {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| self.try_optimize_node(node))
    }
}

impl RewriteOffset {
    const DEFAULT_COLUMN_NAME: &'static str = "__internal_offset_row_number__";

    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Offset(LogicalOffset { input, offset, .. }) => {
                // 1. Create RowNumber Node
                let row_number = Arc::new(
                    LogicalPlan::MonotonicallyIncreasingId(RowNumber::try_new(
                        input.clone(),
                        Some(Self::DEFAULT_COLUMN_NAME),
                    )?)
                    .with_new_children(&[input.clone()]),
                );

                // 2. Create Filter Node
                let filter = Arc::new(
                    LogicalPlan::Filter(LogicalFilter::try_new(
                        row_number.clone(),
                        resolved_col(Self::DEFAULT_COLUMN_NAME).gt_eq(lit(*offset)),
                    )?)
                    .with_new_children(&[row_number]),
                );

                // 3. Create Project Node
                let project = Arc::new(
                    LogicalPlan::Project(LogicalProject::try_new(
                        filter.clone(),
                        filter
                            .schema()
                            .field_names()
                            .filter(|name| **name != *Self::DEFAULT_COLUMN_NAME)
                            .map(resolved_col)
                            .collect::<Vec<_>>(),
                    )?)
                    .with_new_children(&[filter.clone()]),
                );

                Ok(Transformed::yes(project))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_dsl::{lit, resolved_col};
    use daft_schema::{dtype::DataType, field::Field};

    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::RewriteOffset,
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
        LogicalPlan,
    };

    /// Helper that creates an optimizer with the RewriteOffset rule registered, optimizes
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
                vec![Box::new(RewriteOffset::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    #[test]
    fn test_rewrite_offset() -> DaftResult<()> {
        let offset = 7;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone()).offset(offset)?.build();
        let expected = dummy_scan_node(scan_op)
            .add_monotonically_increasing_id(Some(RewriteOffset::DEFAULT_COLUMN_NAME))?
            .filter(resolved_col(RewriteOffset::DEFAULT_COLUMN_NAME).gt_eq(lit(offset)))?
            .select(vec![resolved_col("a"), resolved_col("b")])?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn test_rewrite_offsets() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .offset(1)?
            .offset(3)?
            .offset(7)?
            .build();
        let expected = dummy_scan_node(scan_op)
            .add_monotonically_increasing_id(Some(RewriteOffset::DEFAULT_COLUMN_NAME))?
            .filter(resolved_col(RewriteOffset::DEFAULT_COLUMN_NAME).gt_eq(lit(1u64)))?
            .select(vec![resolved_col("a"), resolved_col("b")])?
            .add_monotonically_increasing_id(Some(RewriteOffset::DEFAULT_COLUMN_NAME))?
            .filter(resolved_col(RewriteOffset::DEFAULT_COLUMN_NAME).gt_eq(lit(3u64)))?
            .select(vec![resolved_col("a"), resolved_col("b")])?
            .add_monotonically_increasing_id(Some(RewriteOffset::DEFAULT_COLUMN_NAME))?
            .filter(resolved_col(RewriteOffset::DEFAULT_COLUMN_NAME).gt_eq(lit(7u64)))?
            .select(vec![resolved_col("a"), resolved_col("b")])?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
