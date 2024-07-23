use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::Project;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorPoolProject {
    // Internal representation of the projection as a non-actor pool projection
    pub project: Project,
    pub num_actors: u32,
}

impl ActorPoolProject {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "ActorPoolProject: {}",
            self.project
                .projection
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        res.push(format!("Num actors = {}", self.num_actors,));
        res.push(format!(
            "Clustering spec = {{ {} }}",
            self.project.clustering_spec.multiline_display().join(", ")
        ));
        let resource_request = self.project.resource_request.multiline_display();
        if !resource_request.is_empty() {
            res.push(format!(
                "Resource request = {{ {} }}",
                resource_request.join(", ")
            ));
        }
        res
    }
}
