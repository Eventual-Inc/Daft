use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

/// Limit task op.
#[derive(Debug)]
pub struct LimitOp {
    limit: usize,
    resource_request: ResourceRequest,
}

impl LimitOp {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            resource_request: ResourceRequest::default_cpu(),
        }
    }
}

impl PartitionTaskOp for LimitOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert_eq!(inputs.len(), 1);
        let input = inputs.iter().next().unwrap();
        let out = input.head(self.limit)?;
        Ok(vec![Arc::new(out)])
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(
        &self,
        input_meta: &[PartitionMetadata],
    ) -> PartitionMetadata {
        assert_eq!(input_meta.len(), 1);
        let input_meta = &input_meta[0];
        input_meta.with_num_rows(Some(self.limit))
    }

    fn name(&self) -> &str {
        "LimitOp"
    }
}
