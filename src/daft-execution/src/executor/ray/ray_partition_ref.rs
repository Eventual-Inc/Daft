use std::sync::Arc;

use daft_micropartition::MicroPartition;
use pyo3::PyObject;

use crate::partition::partition_ref::{PartitionMetadata, PartitionRef};

#[derive(Debug, Clone)]
pub struct RayPartitionRef {
    partition: PyObject,
    metadata: PyObject,
}

impl RayPartitionRef {
    pub fn new(partition: PyObject, metadata: PyObject) -> Self {
        Self {
            partition,
            metadata,
        }
    }
}

impl PartitionRef for RayPartitionRef {
    fn metadata(&self) -> PartitionMetadata {
        // TODO(Clark): Fetch Ray object holding metadata.
        todo!()
    }
    fn partition(&self) -> Arc<MicroPartition> {
        // TODO(Clark): Fetch Ray object holding partition.
        todo!()
    }
}
