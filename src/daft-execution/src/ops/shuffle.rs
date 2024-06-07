use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

fn get_random_u64() -> u64 {
    rand::random()
}

/// Fanout hash task op.
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
            resource_request: ResourceRequest::default_cpu(),
        }
    }
}

impl PartitionTaskOp for FanoutHashOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert!(inputs.len() == 1);
        let inputs = inputs.iter().next().unwrap();
        if self.num_outputs == 1 {
            return Ok(vec![inputs.clone()]);
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

/// Fanout random task op.
#[derive(Debug)]
pub struct FanoutRandomOp {
    num_outputs: usize,
    partition_idx: AtomicI64,
    resource_request: ResourceRequest,
}

impl FanoutRandomOp {
    pub fn new(num_outputs: usize) -> Self {
        Self {
            num_outputs,
            partition_idx: AtomicI64::new(-1),
            resource_request: ResourceRequest::default_cpu(),
        }
    }
}

impl PartitionTaskOp for FanoutRandomOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert!(inputs.len() == 1);
        let inputs = inputs.iter().next().unwrap();
        // Use partition index as seed.
        let seed = self.partition_idx.load(Ordering::SeqCst);
        let partitioned = inputs.partition_by_random(self.num_outputs, seed as u64)?;
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

    fn with_input_metadata(&self, _: &[PartitionMetadata]) {
        self.partition_idx.fetch_add(1, Ordering::SeqCst);
    }

    fn name(&self) -> &str {
        "FanoutHashOp"
    }
}

/// Reduce merge task op.
#[derive(Debug)]
pub struct ReduceMergeOp {
    num_inputs: usize,
    resource_request: ResourceRequest,
}

impl ReduceMergeOp {
    pub fn new(num_inputs: usize) -> Self {
        Self {
            num_inputs,
            resource_request: ResourceRequest::default_cpu(),
        }
    }
}

impl PartitionTaskOp for ReduceMergeOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
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
