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

/// This rule is used to replace `offset` with `project + filter + row_number` to implement offset
/// semantics. Transforms:
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
pub struct ImplementOffset {}

impl ImplementOffset {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ImplementOffset {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| self.try_optimize_node(node))
    }
}

impl ImplementOffset {
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
