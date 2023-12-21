use crate::PartitionSpec;
use daft_core::schema::SchemaRef;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmptyScan {
    pub schema: SchemaRef,
    pub partition_spec: Arc<PartitionSpec>,
}

impl EmptyScan {
    pub(crate) fn new(schema: SchemaRef, partition_spec: Arc<PartitionSpec>) -> Self {
        Self {
            schema,
            partition_spec,
        }
    }
}
