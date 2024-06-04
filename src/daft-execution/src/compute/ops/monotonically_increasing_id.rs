use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::compute::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

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
            resource_request: ResourceRequest::new_internal(Some(1.0), None, None),
        }
    }
}

impl PartitionTaskOp for MonotonicallyIncreasingIdOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert_eq!(inputs.len(), 1);
        let input = inputs.into_iter().next().unwrap();
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
