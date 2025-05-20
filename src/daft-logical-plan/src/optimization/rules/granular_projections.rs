use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{
    functions::{FunctionExpr, ScalarFunction},
    Expr,
};
use itertools::Itertools;

use super::OptimizerRule;
use crate::{ops::Project, LogicalPlan};

/// This rule will split projections into multiple projections such that expressions that
/// need their own granular morsel sizing will be isolated. Right now, those would be
/// URL downloads and Python UDFs, but this may be extended in the future.
#[derive(Debug)]
pub struct GranularProjections {}

impl OptimizerRule for GranularProjections {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_up(|node| self.try_optimize_node(node))
    }
}

impl GranularProjections {
    pub fn new() -> Self {
        Self {}
    }

    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let LogicalPlan::Project(Project {
            projection, input, ..
        }) = plan.as_ref()
        else {
            return Ok(Transformed::no(plan));
        };

        eprintln!("SplitExpensiveProjections");

        let mut rewritten = vec![];

        // Check if the projection has "expensive" operations
        for expr in projection {
            let mut split = vec![];

            let res = expr.clone().transform_up(|e| {
                // TODO: Should be have a helper method or something to identify these
                // operations that need to be treated specially. Esp for future?
                if matches!(
                    e.as_ref(),
                    Expr::Function {
                        func: FunctionExpr::Python(..),
                        ..
                    }
                ) || matches!(
                    e.as_ref(),
                    Expr::ScalarFunction(ScalarFunction { udf, .. }) if udf.name() == "url_download"
                ) {
                    // Split and save child expression
                    let child_name = (split.len()).to_string();
                    let child = e.children().first().unwrap().alias(child_name.as_str());
                    split.push(child);

                    let expr = e.with_new_children(vec![Arc::new(Expr::Column(
                        daft_dsl::Column::Resolved(daft_dsl::ResolvedColumn::Basic(
                            child_name.into(),
                        )),
                    ))]);

                    return Ok(Transformed::yes(Arc::new(expr)));
                }

                if !e.children().is_empty()
                    && (matches!(
                        e.children().first().unwrap().as_ref(),
                        Expr::Function {
                            func: FunctionExpr::Python(..),
                            ..
                        }
                    ) || matches!(
                        e.children().first().unwrap().as_ref(),
                        Expr::ScalarFunction(ScalarFunction { udf, .. }) if udf.name() == "download"
                    ))
                {
                    // Split and save child expression
                    let child_name = (split.len()).to_string();
                    let child = e.children().first().unwrap().alias(child_name.as_str());
                    split.push(child);

                    let expr = e.with_new_children(vec![Arc::new(Expr::Column(
                        daft_dsl::Column::Resolved(daft_dsl::ResolvedColumn::Basic(
                            child_name.into(),
                        )),
                    ))]);

                    return Ok(Transformed::yes(Arc::new(expr)));
                }

                Ok(Transformed::no(e))
            })?;

            split.push(res.data);
            rewritten.push(split);
        }

        if rewritten.iter().all(|x| x.is_empty()) {
            return Ok(Transformed::no(plan));
        }

        // Create new projections for each of the split expressions
        let max_projections = rewritten.iter().map(|x| x.len()).max().unwrap_or(1);

        let mut last_child = input.clone();
        for i in 0..max_projections {
            let exprs = rewritten
                .iter()
                .map(|split| {
                    split.get(i).cloned().unwrap_or_else(|| {
                        let passthrough_name = split.last().unwrap().name();

                        Arc::new(Expr::Column(daft_dsl::Column::Resolved(
                            daft_dsl::ResolvedColumn::Basic(passthrough_name.into()),
                        )))
                    })
                })
                .collect_vec();

            last_child = Arc::new(LogicalPlan::Project(
                Project::try_new(last_child, exprs).unwrap(),
            ));
        }

        Ok(Transformed::yes(last_child))
    }
}
