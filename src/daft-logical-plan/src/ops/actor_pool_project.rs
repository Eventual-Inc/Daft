use std::sync::Arc;

use common_error::DaftError;
use common_resource_request::ResourceRequest;
use common_treenode::TreeNode;
use daft_dsl::{
    functions::{
        python::{get_concurrency, get_resource_request, PythonUDF, StatefulPythonUDF},
        FunctionExpr,
    },
    Expr, ExprRef, ExprResolver,
};
use daft_schema::schema::{Schema, SchemaRef};
use itertools::Itertools;
use snafu::ResultExt;

use crate::{
    logical_plan::{CreationSnafu, Error, Result},
    stats::StatsState,
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ActorPoolProject {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub projection: Vec<ExprRef>,
    pub projected_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl ActorPoolProject {
    pub(crate) fn try_new(input: Arc<LogicalPlan>, projection: Vec<ExprRef>) -> Result<Self> {
        let expr_resolver = ExprResolver::builder().allow_stateful_udf(true).build();
        let (projection, fields) = expr_resolver
            .resolve(projection, input.schema().as_ref())
            .context(CreationSnafu)?;

        let num_stateful_udf_exprs: usize = projection
            .iter()
            .map(|expr| {
                let mut num_stateful_udfs = 0;
                expr.apply(|e| {
                    if matches!(
                        e.as_ref(),
                        Expr::Function {
                            func: FunctionExpr::Python(PythonUDF::Stateful(_)),
                            ..
                        }
                    ) {
                        num_stateful_udfs += 1;
                    }
                    Ok(common_treenode::TreeNodeRecursion::Continue)
                })
                .unwrap();
                num_stateful_udfs
            })
            .sum();
        if !num_stateful_udf_exprs == 1 {
            return Err(Error::CreationError { source: DaftError::InternalError(format!("Expected ActorPoolProject to have exactly 1 stateful UDF expression but found: {num_stateful_udf_exprs}")) });
        }

        let projected_schema = Schema::new(fields).context(CreationSnafu)?.into();

        Ok(Self {
            input,
            projection,
            projected_schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn resource_request(&self) -> Option<ResourceRequest> {
        get_resource_request(self.projection.as_slice())
    }

    pub fn concurrency(&self) -> usize {
        get_concurrency(self.projection.as_slice())
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("ActorPoolProject:".to_string());
        res.push(format!(
            "Projection = [{}]",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "UDFs = [{}]",
            self.projection
                .iter()
                .flat_map(|proj| {
                    let mut udf_names = vec![];
                    proj.apply(|e| {
                        if let Expr::Function {
                            func:
                                FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
                                    name,
                                    ..
                                })),
                            ..
                        } = e.as_ref()
                        {
                            udf_names.push(name.clone());
                        }
                        Ok(common_treenode::TreeNodeRecursion::Continue)
                    })
                    .unwrap();
                    udf_names
                })
                .join(", ")
        ));
        res.push(format!("Concurrency = {}", self.concurrency()));
        if let Some(resource_request) = self.resource_request() {
            let multiline_display = resource_request.multiline_display();
            res.push(format!(
                "Resource request = {{ {} }}",
                multiline_display.join(", ")
            ));
        }
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
