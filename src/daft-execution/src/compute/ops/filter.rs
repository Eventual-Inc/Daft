use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

#[derive(Debug)]
pub struct FilterOp {
    predicate: Vec<ExprRef>,
    resource_request: ResourceRequest,
}

impl FilterOp {
    pub fn new(predicate: Vec<ExprRef>) -> Self {
        Self {
            predicate,
            resource_request: ResourceRequest::new_internal(Some(1.0), None, None),
        }
    }
}

impl PartitionTaskOp for FilterOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert_eq!(inputs.len(), 1);
        let input = inputs.into_iter().next().unwrap();
        let out = input.filter(self.predicate.as_slice())?;
        Ok(vec![Arc::new(out)])
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        "FilterOp"
    }
}
