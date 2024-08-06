use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::{
    functions::{python::PythonUDF, FunctionExpr},
    Expr,
};

use crate::{logical_ops::Project, LogicalPlan};

use super::{ApplyOrder, OptimizerRule, Transformed};

#[derive(Default, Debug)]
pub struct SplitActorPoolProjects {}

/// Implement SplitActorPoolProjects as an OptimizerRule which will:
///
/// 1. Go top-down from the root of the LogicalPlan
/// 2. Whenever it sees a Project, it will iteratively split it into the necessary Project/ActorPoolProject sequences,
///     depending on the underlying Expressions' layouts of StatefulPythonUDFs.
///
/// The general idea behind the splitting is that this is a greedy algorithm which will:
/// * Skim off the top of every expression in the projection to generate "stages" for every expression
/// * Generate Project/ActorPoolProject nodes based on those stages (coalesce non-stateful stages into a Project, and run the stateful stages as ActorPoolProjects sequentially)
/// * Loop until every expression in the projection has been exhausted
///
/// For a given expression tree, skimming a Stage off the top entails:
/// 1. Iterate down the root of the tree, stopping whenever we encounter a StatefulUDF expression
/// 2. If the current stage is rooted at a StatefulUDF expression, then replace its children with Expr::Columns and return the StatefulUDF expression as its own stage
/// 3. Otherwise, the current stage is not a StatefulUDF expression: chop off any StatefulUDF children and replace them with Expr::Columns
impl OptimizerRule for SplitActorPoolProjects {
    fn apply_order(&self) -> ApplyOrder {
        ApplyOrder::TopDown
    }

    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Project(projection) => self.try_optimize_project(projection, plan.clone()),
            // TODO: Figure out how to split other nodes as well such as Filter, Agg etc
            _ => Ok(Transformed::No(plan)),
        }
    }
}

impl SplitActorPoolProjects {
    fn try_optimize_project(
        &self,
        projection: &Project,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // Simple common case: no stateful UDFs at all and we have no transformations
        let no_stateful_udfs = projection.projection.iter().all(|e| {
            !matches!(
                e.as_ref(),
                Expr::Function {
                    func: FunctionExpr::Python(PythonUDF::Stateful(_)),
                    ..
                }
            )
        });
        if no_stateful_udfs {
            return Ok(Transformed::No(plan));
        }

        todo!("implement splitting");
    }
}
