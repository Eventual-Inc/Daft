use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

/// Project task op, applying a projection to its input.
#[derive(Debug)]
pub struct ProjectOp {
    projection: Vec<ExprRef>,
    resource_request: ResourceRequest,
}

impl ProjectOp {
    pub fn new(projection: Vec<ExprRef>, resource_request: ResourceRequest) -> Self {
        Self {
            projection,
            resource_request,
        }
    }
}

impl PartitionTaskOp for ProjectOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert_eq!(inputs.len(), 1);
        let input = inputs.iter().next().unwrap();
        let out = input.eval_expression_list(self.projection.as_slice())?;
        Ok(vec![Arc::new(out)])
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        "ProjectOp"
    }
}
