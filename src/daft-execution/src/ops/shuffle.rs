use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

fn get_random_u64() -> u64 {
    rand::random()
}

#[derive(Debug)]
pub struct FanoutHashOp {
    num_outputs: usize,
    partition_by: Vec<ExprRef>,
    resource_request: ResourceRequest,
}

impl FanoutHashOp {
    pub fn new(num_outputs: usize, partition_by: Vec<ExprRef>) -> Self {
        Self {
            num_outputs,
            partition_by,
            resource_request: ResourceRequest::new_internal(Some(1.0), None, None),
        }
    }
}

impl PartitionTaskOp for FanoutHashOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert!(inputs.len() == 1);
        let inputs = inputs.into_iter().next().unwrap();
        if self.num_outputs == 1 {
            return Ok(vec![inputs]);
        }
        let partitioned = inputs.partition_by_hash(&self.partition_by, self.num_outputs)?;
        Ok(partitioned.into_iter().map(Arc::new).collect::<Vec<_>>())
    }

    fn num_outputs(&self) -> usize {
        self.num_outputs
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        "FanoutHashOp"
    }
}

#[derive(Debug)]
pub struct FanoutRandomOp {
    num_outputs: usize,
    resource_request: ResourceRequest,
}

impl FanoutRandomOp {
    pub fn new(num_outputs: usize) -> Self {
        Self {
            num_outputs,
            resource_request: ResourceRequest::new_internal(Some(1.0), None, None),
        }
    }
}

impl PartitionTaskOp for FanoutRandomOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert!(inputs.len() == 1);
        let inputs = inputs.into_iter().next().unwrap();
        let seed = get_random_u64();
        let partitioned = inputs.partition_by_random(self.num_outputs, seed)?;
        Ok(partitioned.into_iter().map(Arc::new).collect::<Vec<_>>())
    }

    fn num_outputs(&self) -> usize {
        self.num_outputs
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        "FanoutHashOp"
    }
}

#[derive(Debug)]
pub struct ReduceMergeOp {
    num_inputs: usize,
    resource_request: ResourceRequest,
}

impl ReduceMergeOp {
    pub fn new(num_inputs: usize) -> Self {
        Self {
            num_inputs,
            resource_request: ResourceRequest::new_internal(Some(1.0), None, None),
        }
    }
}

impl PartitionTaskOp for ReduceMergeOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: Vec<Arc<Self::Input>>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let inputs = inputs
            .iter()
            .map(|input| input.as_ref())
            .collect::<Vec<_>>();
        Ok(vec![Arc::new(MicroPartition::concat(inputs.as_slice())?)])
    }

    fn num_inputs(&self) -> usize {
        self.num_inputs
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        "ReduceMergeOp"
    }
}
