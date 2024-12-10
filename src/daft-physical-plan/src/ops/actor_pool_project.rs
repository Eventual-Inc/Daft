use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use common_treenode::TreeNode;
use daft_dsl::{
    count_actor_pool_udfs,
    functions::{
        python::{get_concurrency, get_resource_request, PythonUDF},
        FunctionExpr,
    },
    Expr, ExprRef,
};
use daft_logical_plan::partitioning::{translate_clustering_spec, ClusteringSpec};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{impl_default_tree_display, PhysicalPlanRef};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorPoolProject {
    pub input: PhysicalPlanRef,
    pub projection: Vec<ExprRef>,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl ActorPoolProject {
    pub(crate) fn try_new(input: PhysicalPlanRef, projection: Vec<ExprRef>) -> DaftResult<Self> {
        let clustering_spec = translate_clustering_spec(input.clustering_spec(), &projection);

        let num_actor_pool_udfs: usize = count_actor_pool_udfs(&projection);
        if !num_actor_pool_udfs == 1 {
            return Err(DaftError::InternalError(format!("Expected ActorPoolProject to have exactly 1 actor pool UDF expression but found: {num_actor_pool_udfs}")));
        }

        Ok(Self {
            input,
            projection,
            clustering_spec,
        })
    }

    pub fn resource_request(&self) -> Option<ResourceRequest> {
        get_resource_request(self.projection.as_slice())
    }

    /// Retrieves the concurrency of this ActorPoolProject
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
                                FunctionExpr::Python(PythonUDF {
                                    name,
                                    concurrency: Some(_),
                                    ..
                                }),
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

impl_default_tree_display!(ActorPoolProject);
