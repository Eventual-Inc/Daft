use crate::{source_info::InMemoryInfo, ClusteringSpec};
use daft_core::schema::SchemaRef;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InMemoryScan {
    pub schema: SchemaRef,
    pub in_memory_info: InMemoryInfo,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl InMemoryScan {
    pub(crate) fn new(
        schema: SchemaRef,
        in_memory_info: InMemoryInfo,
        clustering_spec: Arc<ClusteringSpec>,
    ) -> Self {
        Self {
            schema,
            in_memory_info,
            clustering_spec,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("InMemoryScan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!("Size bytes = {}", self.in_memory_info.size_bytes,));
        res.push(format!(
            "Clustering spec = {{ {} }}",
            self.clustering_spec.multiline_display().join(", ")
        ));
        res
    }
}
