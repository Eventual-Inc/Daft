use crate::{source_info::InMemoryInfo, PartitionSpec};
use daft_core::schema::SchemaRef;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
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
}
