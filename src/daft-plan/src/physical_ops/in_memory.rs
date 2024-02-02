use crate::{source_info::InMemoryInfo, PartitionSpec};
use daft_core::schema::SchemaRef;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InMemoryScan {
    pub schema: SchemaRef,
    pub in_memory_info: InMemoryInfo,
    pub partition_spec: Arc<PartitionSpec>,
}

impl InMemoryScan {
    pub(crate) fn new(
        schema: SchemaRef,
        in_memory_info: InMemoryInfo,
        partition_spec: Arc<PartitionSpec>,
    ) -> Self {
        Self {
            schema,
            in_memory_info,
            partition_spec,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("InMemoryScan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!("Size bytes = {}", self.in_memory_info.size_bytes,));
        res.push(format!(
            "Partition spec = {{ {} }}",
            self.partition_spec.multiline_display().join(", ")
        ));
        res
    }
}
