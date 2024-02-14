use crate::ClusteringSpec;
use daft_core::schema::SchemaRef;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EmptyScan {
    pub schema: SchemaRef,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl EmptyScan {
    pub(crate) fn new(schema: SchemaRef, clustering_spec: Arc<ClusteringSpec>) -> Self {
        Self {
            schema,
            clustering_spec,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("EmptyScan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!(
            "Clustering spec = {{ {} }}",
            self.clustering_spec.multiline_display().join(", ")
        ));
        res
    }
}
