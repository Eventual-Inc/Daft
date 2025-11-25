use std::{num::NonZeroUsize, sync::Arc};

use common_error::DaftResult;
use common_resource_request::ResourceRequest;
use common_treenode::TreeNode;
use daft_dsl::{
    Expr, ExprRef,
    functions::{
        FunctionExpr,
        python::{LegacyPythonUDF, UDFProperties},
    },
};
use daft_logical_plan::partitioning::{ClusteringSpec, translate_clustering_spec};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{PhysicalPlanRef, impl_default_tree_display};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorPoolProject {
    pub input: PhysicalPlanRef,
    pub projection: Vec<ExprRef>,
    pub udf_properties: UDFProperties,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl ActorPoolProject {
    pub(crate) fn try_new(
        input: PhysicalPlanRef,
        projection: Vec<ExprRef>,
        udf_properties: UDFProperties,
    ) -> DaftResult<Self> {
        let clustering_spec = translate_clustering_spec(input.clustering_spec(), &projection);

        Ok(Self {
            input,
            projection,
            udf_properties,
            clustering_spec,
        })
    }

    pub fn resource_request(&self) -> Option<ResourceRequest> {
        self.udf_properties.resource_request.clone()
    }

    /// Retrieves the concurrency of this ActorPoolProject
    pub fn concurrency(&self) -> NonZeroUsize {
        self.udf_properties
            .concurrency
            .expect("ActorPoolProject should have concurrency specified")
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
                                FunctionExpr::Python(LegacyPythonUDF {
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
