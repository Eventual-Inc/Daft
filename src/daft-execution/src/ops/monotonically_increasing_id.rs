use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

/// Monotonically increasing ID task op.
#[derive(Debug)]
pub struct MonotonicallyIncreasingIdOp {
    column_name: String,
    num_partitions: AtomicI64,
    resource_request: ResourceRequest,
}

impl MonotonicallyIncreasingIdOp {
    pub fn new(column_name: String) -> Self {
        Self {
            column_name,
            num_partitions: AtomicI64::new(-1),
            resource_request: ResourceRequest::default_cpu(),
        }
    }
}

impl PartitionTaskOp for MonotonicallyIncreasingIdOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert_eq!(inputs.len(), 1);
        let input = inputs.iter().next().unwrap();
        let out = input.add_monotonically_increasing_id(
            self.num_partitions.load(Ordering::SeqCst) as u64,
            &self.column_name,
        )?;
        Ok(vec![Arc::new(out)])
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn with_input_metadata(&self, _: &[PartitionMetadata]) {
        self.num_partitions.fetch_add(1, Ordering::SeqCst);
    }

    fn name(&self) -> &str {
        "MonotonicallyIncreasingIdOp"
    }
}
