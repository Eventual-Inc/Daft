use crate::PartitionSpec;
use daft_core::schema::SchemaRef;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("EmptyScan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!(
            "Partition spec = {{ {} }}",
            self.partition_spec.multiline_display().join(", ")
        ));
        res
    }
}
