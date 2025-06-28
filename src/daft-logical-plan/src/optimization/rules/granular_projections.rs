use std::{any::TypeId, collections::HashSet, sync::Arc};

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{functions::ScalarFunction, resolved_col, Expr};
use daft_functions_uri::download::UrlDownload;
use itertools::Itertools;

use super::OptimizerRule;
use crate::{ops::Project, LogicalPlan};

/// This rule will split projections into multiple projections such that expressions that
/// need their own granular morsel sizing will be isolated. Right now, those would be
/// URL downloads, but this may be extended in the future to other functions and Python UDFs.
///
/// Example of Original Plan:
///     3) Sink
///     2) Project(decode(url_download("s3://bucket/" + key)) as image, name)
///     1) Source
///
/// New Plan:
///     5) Sink
///     4) Project(decode(data) as image, name)
///     3) Project(url_download(url), name)
///     2) Project("s3://bucket/" + key as url, name)
///     1) Source
#[derive(Debug)]
pub struct SplitGranularProjection {}

impl OptimizerRule for SplitGranularProjection {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_up(|node| self.try_optimize_node(node))
    }
}

impl SplitGranularProjection {
    pub fn new() -> Self {
        Self {}
    }

    fn requires_granular_morsel_sizing(expr: &Expr) -> bool {
        // TODO: Add Python UDFs as well, but need to handle multiple args better
        // As well as good testing
        matches!(
            expr,
            Expr::ScalarFunction(ScalarFunction { udf, .. }) if udf.as_ref().type_id() == TypeId::of::<UrlDownload>()
        )
    }

    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // Only apply to Project nodes, skip others
        let LogicalPlan::Project(Project {
            projection,
            input,
            projected_schema,
            ..
        }) = plan.as_ref()
        else {
            return Ok(Transformed::no(plan));
        };

        // Contains all of expression pieces for each projection column
        let mut all_split_exprs = vec![];

        // Check if the projection has operations to isolate
        for expr in projection {
            let mut split_exprs = vec![];

            let res = expr.clone().transform_up(|e| {
                // If we encounter a function that needs to be isolated, we split off its children
                // and replace with column references.

                // TODO: Should be have a helper method or something to identify these
                // operations that need to be treated specially. Esp for future?
                if Self::requires_granular_morsel_sizing(e.as_ref()) {
                    // Split and save child expressions
                    let mut children = vec![];
                    let mut changed = false;
                    for child in e.children() {
                        match child.as_ref() {
                            // If child is just a reference, don't do anything
                            Expr::Column(_) | Expr::Literal(_) => children.push(child),
                            _ => {
                                changed = true;
                                // Child may not have an alias, so we need to generate a new one
                                // TODO: Remove with ordinals
                                let child_name = format!("id-{}", uuid::Uuid::new_v4());
                                let child = child.alias(child_name.clone());
                                split_exprs.push(child);

                                children.push(resolved_col(child_name));
                            }
                        }
                    }

                    if changed {
                        return Ok(Transformed::yes(Arc::new(e.with_new_children(children))));
                    } else {
                        return Ok(Transformed::no(e));
                    }
                }

                // If we encounter a function with a child to treat specially, we split off said child
                // and replace with a column reference.
                if !e.children().is_empty() {
                    let mut new_children = e.children().iter().cloned().collect_vec();
                    let mut changed = false;

                    for (idx, child) in e.children().iter().enumerate() {
                        if Self::requires_granular_morsel_sizing(child) {
                            changed = true;
                            // Split and save child expression
                            // Child may not have an alias, so we need to generate a new one
                            // TODO: Remove with ordinals
                            let child_name = format!("id-{}", uuid::Uuid::new_v4());
                            let child = child.alias(child_name.clone());
                            split_exprs.push(child);
                            new_children[idx] = resolved_col(child_name);
                        }
                    }

                    if changed {
                        let expr = e.with_new_children(new_children);
                        return Ok(Transformed::yes(Arc::new(expr)));
                    } else {
                        return Ok(Transformed::no(e));
                    }
                }

                Ok(Transformed::no(e))
            })?;

            // Push the top level expression that was changed
            split_exprs.push(res.data);
            all_split_exprs.push(split_exprs);
        }

        // In this case, none of the projection columns were split or changed, so we can just continue
        if all_split_exprs.iter().all(|x| x.len() == 1) {
            return Ok(Transformed::no(plan));
        }

        // Create new Project nodes. There will be as many Project nodes as there are
        // maximum # of split expressions for one of the original projection columns
        let max_projections = all_split_exprs.iter().map(|x| x.len()).max().unwrap_or(1);

        // Construct new Project nodes in the following way:
        // Original: Project(input, [e1, e2, e3])
        // Split: [e1, e2, e3] -> [[e11, e12, e13], [e21], [e31, e32]]
        // New:
        //  - Project(input, [e11, e21, e31])
        //  - Project(input, [e12, passthrough, e32])
        //  - Project(input, [e13, passthrough, passthrough])
        // Passthrough means just passing the original column through unchanged

        let mut last_child = input.clone();
        let input_cols = input
            .schema()
            .field_names()
            .map(Arc::from)
            .collect::<Vec<Arc<str>>>();

        for i in 0..max_projections {
            let mut out_names = HashSet::new();
            let mut out_exprs = vec![];

            for split in &all_split_exprs {
                let expr = split.get(i).cloned().unwrap_or_else(|| {
                    let passthrough_name = split.last().unwrap().name();
                    resolved_col(passthrough_name)
                });
                out_names.insert(expr.name().to_string());
                out_exprs.push(expr);
            }

            for name in &input_cols {
                if !out_names.contains(name.as_ref()) {
                    out_names.insert(name.to_string());
                    out_exprs.push(resolved_col(name.clone()));
                }
            }

            last_child = Arc::new(LogicalPlan::Project(
                Project::try_new(last_child, out_exprs).unwrap(),
            ));
        }

        // Only expose the columns that are in the original projection, not the inputs
        let last_fields = last_child
            .schema()
            .field_names()
            .take(projection.len())
            // Ensure that the output column names match the original projection
            .zip(projected_schema.field_names())
            .map(|c| resolved_col(Arc::from(c.0)).alias(Arc::from(c.1)))
            .collect_vec();

        last_child = Arc::new(LogicalPlan::Project(
            Project::try_new(last_child, last_fields).unwrap(),
        ));

        Ok(Transformed::yes(last_child))
    }
}

#[cfg(test)]
mod tests {

    use common_scan_info::Pushdowns;
    use daft_dsl::{lit, Column, ExprRef, ResolvedColumn};
    use daft_functions_binary::{BinaryConcat, BinaryDecode, Codec};
    use daft_functions_uri::download::UrlDownload;
    use daft_functions_utf8::{capitalize, lower, Capitalize};
    use daft_schema::{dtype::DataType, field::Field};

    use super::*;
    use crate::test::{dummy_scan_node_with_pushdowns, dummy_scan_operator};

    #[test]
    fn test_noop() -> DaftResult<()> {
        // Test when there is an unrelated project

        let plan = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("name", DataType::Utf8)]),
            Pushdowns::default(),
        )
        .with_columns(vec![capitalize(resolved_col("name"))])
        .unwrap()
        .build();

        let optimizer = SplitGranularProjection::new();
        let new_plan = optimizer.try_optimize(plan.clone())?;

        assert!(!new_plan.transformed);
        assert_eq!(new_plan.data.as_ref(), plan.as_ref());
        Ok(())
    }

    #[test]
    fn test_noop_with_url() -> DaftResult<()> {
        // Test when there is an already isolated project expression

        let plan = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![Field::new("url", DataType::Utf8)]),
            Pushdowns::default(),
        )
        .with_columns(vec![ScalarFunction::new(
            UrlDownload,
            vec![resolved_col("url")],
        )
        .into()])
        .unwrap()
        .build();

        let optimizer = SplitGranularProjection::new();
        let new_plan = optimizer.try_optimize(plan.clone())?;

        assert!(!new_plan.transformed);
        assert_eq!(new_plan.data.as_ref(), plan.as_ref());
        Ok(())
    }

    #[test]
    fn test_split_top() -> DaftResult<()> {
        // Test an actual split
        let plan = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![
                Field::new("name", DataType::Utf8),
                Field::new("url", DataType::Utf8),
            ]),
            Pushdowns::default(),
        )
        .select(vec![
            ExprRef::from(ScalarFunction::new(
                BinaryDecode,
                vec![
                    ScalarFunction::new(UrlDownload, vec![resolved_col("url")]).into(),
                    lit(Codec::Utf8),
                ],
            ))
            .alias("url_data"),
            lower(Expr::Column(Column::Resolved(ResolvedColumn::Basic("name".into()))).arced())
                .alias("name_lower"),
        ])
        .unwrap()
        .build();

        let optimizer = SplitGranularProjection::new();
        let new_plan = optimizer.try_optimize(plan)?;
        assert!(new_plan.transformed);

        assert!(matches!(new_plan.data.as_ref(), LogicalPlan::Project(_)));
        let LogicalPlan::Project(column_prune_project) = new_plan.data.as_ref() else {
            panic!("Expected top project for column pruning");
        };
        assert_eq!(column_prune_project.projection.len(), 2);
        assert_eq!(column_prune_project.projection[0].name(), "url_data");
        assert_eq!(column_prune_project.projection[1].name(), "name_lower");
        assert!(matches!(
            column_prune_project.input.as_ref(),
            LogicalPlan::Project(_)
        ));

        let LogicalPlan::Project(top_project) = column_prune_project.input.as_ref() else {
            panic!("Expected top level project");
        };
        assert_eq!(top_project.projection.len(), 4);
        assert!(matches!(
            top_project.projection[0].as_ref(),
            Expr::Alias(..)
        ));
        let Expr::Alias(func, ..) = top_project.projection[0].as_ref() else {
            panic!("Expected alias");
        };
        assert!(matches!(
            func.as_ref(),
            Expr::ScalarFunction(ScalarFunction { udf, .. }) if udf.as_ref().type_id() == TypeId::of::<BinaryDecode>()
        ));

        // Check that the top level project has a single child, which is a project
        assert!(matches!(
            top_project.input.as_ref(),
            LogicalPlan::Project(_)
        ));
        let LogicalPlan::Project(bottom_project) = top_project.input.as_ref() else {
            panic!("Expected middle level project");
        };
        assert_eq!(bottom_project.projection.len(), 4);
        assert!(matches!(
            bottom_project.projection[0].as_ref(),
            Expr::Alias(..)
        ));
        let Expr::Alias(func, ..) = bottom_project.projection[0].as_ref() else {
            panic!("Expected alias");
        };
        assert!(matches!(
            func.as_ref(),
            Expr::ScalarFunction(ScalarFunction { udf, .. }) if udf.as_ref().type_id() == TypeId::of::<UrlDownload>()
        ));

        // Check that the bottom level project has a single child, which is a source node
        assert!(matches!(
            bottom_project.input.as_ref(),
            LogicalPlan::Source(_)
        ));

        Ok(())
    }

    #[test]
    fn test_split_both() -> DaftResult<()> {
        // Test an actual split
        let plan = dummy_scan_node_with_pushdowns(
            dummy_scan_operator(vec![
                Field::new("name", DataType::Utf8),
                Field::new("url", DataType::Utf8),
                Field::new("extra", DataType::Binary),
            ]),
            Pushdowns::default(),
        )
        .select(vec![
            ExprRef::from(ScalarFunction::new(
                BinaryConcat,
                vec![
                    ScalarFunction::new(UrlDownload, vec![capitalize(resolved_col("url"))]).into(),
                    resolved_col("extra"),
                ],
            ))
            .alias("url_data"),
            lower(Expr::Column(Column::Resolved(ResolvedColumn::Basic("name".into()))).arced())
                .alias("name_lower"),
        ])
        .unwrap()
        .build();

        let optimizer = SplitGranularProjection::new();
        let new_plan = optimizer.try_optimize(plan)?;
        assert!(new_plan.transformed);

        assert!(matches!(new_plan.data.as_ref(), LogicalPlan::Project(_)));
        let LogicalPlan::Project(column_prune_project) = new_plan.data.as_ref() else {
            panic!("Expected top project for column pruning");
        };
        assert_eq!(column_prune_project.projection.len(), 2);
        assert_eq!(column_prune_project.projection[0].name(), "url_data");
        assert_eq!(column_prune_project.projection[1].name(), "name_lower");
        assert!(matches!(
            column_prune_project.input.as_ref(),
            LogicalPlan::Project(_)
        ));

        let LogicalPlan::Project(top_project) = column_prune_project.input.as_ref() else {
            panic!("Expected top level project");
        };
        assert_eq!(top_project.projection.len(), 5);
        assert!(matches!(
            top_project.projection[0].as_ref(),
            Expr::Alias(..)
        ));
        let Expr::Alias(func, ..) = top_project.projection[0].as_ref() else {
            panic!("Expected alias");
        };
        assert!(matches!(
            func.as_ref(),
            Expr::ScalarFunction(ScalarFunction { udf, .. }) if udf.as_ref().type_id() == TypeId::of::<BinaryConcat>()
        ));

        // Check that the top level project has a single child, which is a project
        assert!(matches!(
            top_project.input.as_ref(),
            LogicalPlan::Project(_)
        ));
        let LogicalPlan::Project(middle_project) = top_project.input.as_ref() else {
            panic!("Expected middle level project");
        };
        assert_eq!(middle_project.projection.len(), 5);
        assert!(matches!(
            middle_project.projection[0].as_ref(),
            Expr::Alias(..)
        ));
        let Expr::Alias(func, ..) = middle_project.projection[0].as_ref() else {
            panic!("Expected alias");
        };
        assert!(matches!(
            func.as_ref(),
            Expr::ScalarFunction(ScalarFunction { udf, .. }) if udf.as_ref().type_id() == TypeId::of::<UrlDownload>()
        ));

        // Check that the middle level project has a single child, which is a project
        assert!(matches!(
            middle_project.input.as_ref(),
            LogicalPlan::Project(_)
        ));
        let LogicalPlan::Project(bottom_project) = middle_project.input.as_ref() else {
            panic!("Expected bottom level project");
        };
        assert_eq!(bottom_project.projection.len(), 5);
        assert!(matches!(
            bottom_project.projection[0].as_ref(),
            Expr::Alias(..)
        ));
        let Expr::Alias(func, ..) = bottom_project.projection[0].as_ref() else {
            panic!("Expected alias");
        };
        assert!(matches!(
            func.as_ref(),
            Expr::ScalarFunction(ScalarFunction { udf, .. }) if udf.as_ref().type_id() == TypeId::of::<Capitalize>()
        ));

        // Check that the bottom level project has a single child, which is a source node
        assert!(matches!(
            bottom_project.input.as_ref(),
            LogicalPlan::Source(_)
        ));

        Ok(())
    }

    // TODO: Add test for UDFs, can't create a fake one for testing
}
