use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{ClusteringSpec, PhysicalPlanRef, ResourceRequest};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorPoolProject {
    pub input: PhysicalPlanRef,
    pub stateful_python_udf: daft_dsl::functions::python::StatefulPythonUDF,
    pub resource_request: ResourceRequest,
    pub clustering_spec: Arc<ClusteringSpec>,
    pub num_actors: u32,
}

impl ActorPoolProject {
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
