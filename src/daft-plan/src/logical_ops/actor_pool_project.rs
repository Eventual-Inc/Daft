use std::sync::Arc;

use common_resource_request::ResourceRequest;
use common_treenode::TreeNode;
use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::{
    functions::{
        python::{get_resource_request, PythonUDF, StatefulPythonUDF},
        FunctionExpr,
    },
    resolve_exprs, Expr, ExprRef,
};
use itertools::Itertools;
use snafu::ResultExt;

use crate::{
    logical_plan::{CreationSnafu, Result},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ActorPoolProject {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub projection: Vec<ExprRef>,
    pub projected_schema: SchemaRef,
    pub num_actors: usize,
}

impl ActorPoolProject {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        projection: Vec<ExprRef>,
        num_actors: usize,
    ) -> Result<Self> {
        let (projection, fields) =
            resolve_exprs(projection, input.schema().as_ref()).context(CreationSnafu)?;
        let projected_schema = Schema::new(fields).context(CreationSnafu)?.into();
        Ok(ActorPoolProject {
            input,
            projection,
            projected_schema,
            num_actors,
        })
    }

    pub fn resource_request(&self) -> Option<ResourceRequest> {
        get_resource_request(self.projection.as_slice())
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
        res.push(format!("Num actors = {}", self.num_actors,));
        if let Some(resource_request) = self.resource_request() {
            let multiline_display = resource_request.multiline_display();
            res.push(format!(
                "Resource request = {{ {} }}",
                multiline_display.join(", ")
            ));
        }
        res
    }
}
