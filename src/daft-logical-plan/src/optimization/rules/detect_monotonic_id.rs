use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_dsl::{col, Expr, ExprRef};

use crate::{
    logical_plan::{LogicalPlan, Project},
    ops::MonotonicallyIncreasingId,
    optimization::rules::OptimizerRule,
};

/// Optimization rule that detects monotonically_increasing_id() expressions in Project operations
/// and transforms them into MonotonicallyIncreasingId operations.
#[derive(Debug)]
pub struct DetectMonotonicId;

impl Default for DetectMonotonicId {
    fn default() -> Self {
        Self
    }
}

impl DetectMonotonicId {
    /// Helper function to detect if an expression is a monotonically_increasing_id() call
    fn is_monotonic_id_expr(expr: &ExprRef) -> bool {
        match expr.as_ref() {
            Expr::ScalarFunction(func) => {
                func.is_special_function() && func.name() == "monotonically_increasing_id"
            }
            Expr::Alias(inner, _) => Self::is_monotonic_id_expr(inner),
            _ => false,
        }
    }

    /// Helper function to find all monotonically_increasing_id() expressions in a Project operation
    /// Returns a vector of (column_name, original_expr) pairs for each monotonic ID found
    fn find_monotonic_ids(project: &Project) -> Vec<(String, ExprRef)> {
        let mut result = Vec::new();

        for expr in &project.projection {
            if Self::is_monotonic_id_expr(expr) {
                let column_name = match expr.as_ref() {
                    Expr::Alias(_, name) => name.to_string(),
                    _ => "id".to_string(), // Default name if not aliased
                };
                result.push((column_name, expr.clone()));
            }
        }

        result
    }

    /// Helper function to create a chain of MonotonicallyIncreasingId operations
    fn create_monotonic_chain(
        input: Arc<LogicalPlan>,
        monotonic_ids: Vec<(String, ExprRef)>,
    ) -> DaftResult<Arc<LogicalPlan>> {
        let mut current_plan = input;

        // Create a chain of MonotonicallyIncreasingId operations
        for (column_name, _) in monotonic_ids {
            current_plan = Arc::new(LogicalPlan::MonotonicallyIncreasingId(
                MonotonicallyIncreasingId::try_new(current_plan, Some(&column_name))?,
            ));
        }

        Ok(current_plan)
    }

    /// Helper function to create a new projection list that preserves the original order
    /// and replaces monotonically_increasing_id() calls with column references
    fn create_new_projection(
        original_projection: &[ExprRef],
        _monotonic_ids: &[(String, ExprRef)],
    ) -> Vec<ExprRef> {
        original_projection
            .iter()
            .map(|expr| {
                if Self::is_monotonic_id_expr(expr) {
                    // Find the corresponding column name for this monotonic ID
                    let column_name = match expr.as_ref() {
                        Expr::Alias(_, name) => name.to_string(),
                        _ => "id".to_string(),
                    };
                    // Replace with a column reference
                    col(column_name)
                } else {
                    expr.clone()
                }
            })
            .collect()
    }
}

impl OptimizerRule for DetectMonotonicId {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // First try to optimize any child plans
        let mut transformed = false;
        let optimized_plan = plan.clone().transform_down(|node| {
            match node.as_ref() {
                LogicalPlan::Project(project) => {
                    // Find all monotonically_increasing_id() calls in this Project
                    let monotonic_ids = Self::find_monotonic_ids(project);

                    if !monotonic_ids.is_empty() {
                        transformed = true;
                        // Create a chain of MonotonicallyIncreasingId operations
                        let monotonic_plan = Self::create_monotonic_chain(
                            project.input.clone(),
                            monotonic_ids.clone(),
                        )?;

                        // Create a new projection list that preserves the original order
                        let new_projection =
                            Self::create_new_projection(&project.projection, &monotonic_ids);

                        // Create a new Project operation with the updated projection list
                        let final_plan = Arc::new(LogicalPlan::Project(Project::try_new(
                            monotonic_plan,
                            new_projection,
                        )?));

                        Ok(Transformed::yes(final_plan))
                    } else {
                        Ok(Transformed::no(node))
                    }
                }
                _ => Ok(Transformed::no(node)),
            }
        })?;

        // If any transformations occurred, continue recursing down the tree
        if transformed {
            Ok(Transformed {
                data: optimized_plan.data,
                transformed,
                tnr: TreeNodeRecursion::Continue,
            })
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

#[cfg(test)]
mod tests {
    use common_partitioning::PartitionCacheEntry;
    use daft_core::prelude::*;
    use daft_functions::sequence::monotonically_increasing_id::monotonically_increasing_id;

    use super::*;
    use crate::{
        ops::Source,
        source_info::{InMemoryInfo, SourceInfo},
    };

    fn create_test_plan() -> Arc<LogicalPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]).unwrap());
        Arc::new(LogicalPlan::Source(Source::new(
            schema.clone(),
            Arc::new(SourceInfo::InMemory(InMemoryInfo::new(
                schema,
                "test".to_string(),
                PartitionCacheEntry::new_rust("test".to_string(), Arc::<Vec<()>>::new(vec![])),
                1,
                0,
                0,
                None,
            ))),
        )))
    }

    #[test]
    fn test_detect_monotonic_id_only() {
        let source = create_test_plan();
        let mono_id = monotonically_increasing_id().alias("id");

        let plan = Arc::new(LogicalPlan::Project(
            Project::try_new(source.clone(), vec![mono_id]).unwrap(),
        ));

        let rule = DetectMonotonicId;
        let result = rule.try_optimize(plan).unwrap();

        match result {
            Transformed {
                data: new_plan,
                transformed: true,
                ..
            } => match new_plan.as_ref() {
                LogicalPlan::Project(project) => {
                    assert_eq!(project.projection.len(), 1);
                    match project.input.as_ref() {
                        LogicalPlan::MonotonicallyIncreasingId(monotonic) => {
                            assert!(matches!(monotonic.input.as_ref(), LogicalPlan::Source(_)));
                            assert_eq!(monotonic.column_name, "id");
                        }
                        _ => panic!("Expected MonotonicallyIncreasingId operation"),
                    }
                }
                _ => panic!("Expected Project operation"),
            },
            _ => panic!("Expected transformation to occur"),
        }
    }

    #[test]
    fn test_detect_monotonic_id_with_other_columns() {
        let source = create_test_plan();
        let mono_id = monotonically_increasing_id().alias("id");

        let plan = Arc::new(LogicalPlan::Project(
            Project::try_new(source.clone(), vec![col("a"), mono_id]).unwrap(),
        ));

        let rule = DetectMonotonicId;
        let result = rule.try_optimize(plan).unwrap();

        match result {
            Transformed {
                data: new_plan,
                transformed: true,
                ..
            } => match new_plan.as_ref() {
                LogicalPlan::Project(project) => {
                    assert_eq!(project.projection.len(), 2);
                    match project.input.as_ref() {
                        LogicalPlan::MonotonicallyIncreasingId(monotonic) => {
                            assert!(matches!(monotonic.input.as_ref(), LogicalPlan::Source(_)));
                            assert_eq!(monotonic.column_name, "id");
                        }
                        _ => panic!("Expected MonotonicallyIncreasingId operation"),
                    }
                }
                _ => panic!("Expected Project operation"),
            },
            _ => panic!("Expected transformation to occur"),
        }
    }

    #[test]
    fn test_detect_multiple_monotonic_ids() {
        let source = create_test_plan();
        let mono_id1 = monotonically_increasing_id().alias("id1");
        let mono_id2 = monotonically_increasing_id().alias("id2");

        let plan = Arc::new(LogicalPlan::Project(
            Project::try_new(source.clone(), vec![col("a"), mono_id1, mono_id2]).unwrap(),
        ));

        let rule = DetectMonotonicId;
        let result = rule.try_optimize(plan).unwrap();

        match result {
            Transformed {
                data: new_plan,
                transformed: true,
                ..
            } => match new_plan.as_ref() {
                LogicalPlan::Project(project) => {
                    assert_eq!(project.projection.len(), 3);
                    match project.input.as_ref() {
                        LogicalPlan::MonotonicallyIncreasingId(monotonic2) => {
                            assert_eq!(monotonic2.column_name, "id2");
                            match monotonic2.input.as_ref() {
                                LogicalPlan::MonotonicallyIncreasingId(monotonic1) => {
                                    assert_eq!(monotonic1.column_name, "id1");
                                }
                                _ => panic!("Expected inner MonotonicallyIncreasingId operation"),
                            }
                        }
                        _ => panic!("Expected outer MonotonicallyIncreasingId operation"),
                    }
                }
                _ => panic!("Expected Project operation"),
            },
            _ => panic!("Expected transformation to occur"),
        }
    }

    #[test]
    fn test_no_transformation_needed() {
        let source = create_test_plan();
        let plan = Arc::new(LogicalPlan::Project(
            Project::try_new(source.clone(), vec![col("a")]).unwrap(),
        ));

        let rule = DetectMonotonicId;
        let result = rule.try_optimize(plan.clone()).unwrap();

        match result {
            Transformed {
                transformed: true, ..
            } => panic!("Expected no transformation"),
            Transformed {
                data: unchanged_plan,
                transformed: false,
                ..
            } => {
                assert!(Arc::ptr_eq(&plan, &unchanged_plan));
            }
        }
    }
}
