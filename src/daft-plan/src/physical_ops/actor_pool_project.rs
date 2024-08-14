use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use common_treenode::TreeNode;
use daft_dsl::{
    functions::{
        python::{get_resource_request, PythonUDF, StatefulPythonUDF},
        FunctionExpr,
    },
    Expr, ExprRef,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{partitioning::translate_clustering_spec, ClusteringSpec, PhysicalPlanRef};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorPoolProject {
    pub input: PhysicalPlanRef,
    pub projection: Vec<ExprRef>,
    pub clustering_spec: Arc<ClusteringSpec>,
    pub num_actors: usize,
}

impl ActorPoolProject {
    pub(crate) fn try_new(
        input: PhysicalPlanRef,
        projection: Vec<ExprRef>,
        num_actors: usize,
    ) -> DaftResult<Self> {
        let clustering_spec = translate_clustering_spec(input.clustering_spec(), &projection);

        if !projection.iter().any(|expr| {
            expr.exists(|expr| {
                matches!(
                    expr.as_ref(),
                    Expr::Function {
                        func: FunctionExpr::Python(PythonUDF::Stateful(_)),
                        ..
                    }
                )
            })
        }) {
            return Err(DaftError::InternalError("Cannot create ActorPoolProject from expressions that don't contain a stateful Python UDF".to_string()));
        }

        Ok(ActorPoolProject {
            input,
            projection,
            clustering_spec,
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
        res.push(format!(
            "Clustering spec = {{ {} }}",
            self.clustering_spec.multiline_display().join(", ")
        ));
        let resource_request = self.resource_request().map(|rr| rr.multiline_display());
        if let Some(resource_request) = resource_request
            && !resource_request.is_empty()
        {
            res.push(format!(
                "Resource request = {{ {} }}",
                resource_request.join(", ")
            ));
        } else {
            res.push("Resource request = None".to_string());
        }
        res
    }
}
