use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_dsl::{
    functions::{python::PythonUDF, FunctionExpr},
    Expr, ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::{
    partitioning::translate_clustering_spec, ClusteringSpec, PhysicalPlanRef, ResourceRequest,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorPoolProject {
    pub input: PhysicalPlanRef,
    pub projection: Vec<ExprRef>,
    pub resource_request: ResourceRequest,
    pub clustering_spec: Arc<ClusteringSpec>,
    pub num_actors: usize,
}

impl ActorPoolProject {
    pub(crate) fn try_new(
        input: PhysicalPlanRef,
        projection: Vec<ExprRef>,
        resource_request: ResourceRequest,
        num_actors: usize,
    ) -> DaftResult<Self> {
        let clustering_spec = translate_clustering_spec(input.clustering_spec(), &projection);

        if !projection.iter().any(|expr| {
            matches!(
                expr.as_ref(),
                Expr::Function {
                    func: FunctionExpr::Python(PythonUDF::Stateful(_)),
                    ..
                }
            )
        }) {
            return Err(DaftError::InternalError("Cannot create ActorPoolProject from an expressions that don't contain a stateful Python UDF".to_string()));
        }

        Ok(ActorPoolProject {
            input,
            projection,
            resource_request,
            clustering_spec,
            num_actors,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "ActorPoolProject: {}",
            // TODO: propagate name of UDF
            "TODO",
        ));
        res.push(format!("Num actors = {}", self.num_actors,));
        res.push(format!(
            "Clustering spec = {{ {} }}",
            self.clustering_spec.multiline_display().join(", ")
        ));
        let resource_request = self.resource_request.multiline_display();
        if !resource_request.is_empty() {
            res.push(format!(
                "Resource request = {{ {} }}",
                resource_request.join(", ")
            ));
        }
        res
    }
}
