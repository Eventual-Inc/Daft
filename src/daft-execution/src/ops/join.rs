use std::sync::Arc;

use common_error::DaftResult;
use daft_core::JoinType;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

/// Hash join task op.
#[derive(Debug)]
pub struct HashJoinOp {
    left_on: Vec<ExprRef>,
    right_on: Vec<ExprRef>,
    join_type: JoinType,
    resource_request: ResourceRequest,
}

impl HashJoinOp {
    pub fn new(left_on: Vec<ExprRef>, right_on: Vec<ExprRef>, join_type: JoinType) -> Self {
        Self {
            left_on,
            right_on,
            join_type,
            resource_request: ResourceRequest::default_cpu(),
        }
    }
}

impl PartitionTaskOp for HashJoinOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert_eq!(inputs.len(), 2);
        let mut input_iter = inputs.iter();
        let left = input_iter.next().unwrap();
        let right = input_iter.next().unwrap();
        let out = left.hash_join(
            right.as_ref(),
            &self.left_on,
            &self.right_on,
            self.join_type,
        )?;
        Ok(vec![Arc::new(out)])
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        "HashJoinOp"
    }
}
