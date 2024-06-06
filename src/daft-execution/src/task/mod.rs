use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use daft_plan::ResourceRequest;
use daft_scan::ScanTask;

use crate::ops::PartitionTaskOp;

use crate::partition::{
    partition_ref::{PartitionMetadata, PartitionRef},
    virtual_partition::VirtualPartition,
};

static TASK_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub enum Task<T: PartitionRef> {
    ScanTask(PartitionTask<Arc<ScanTask>>),
    PartitionTask(PartitionTask<T>),
}

impl<T: PartitionRef> Task<T> {
    pub fn resource_request(&self) -> &ResourceRequest {
        match self {
            Self::ScanTask(pt) => pt.resource_request(),
            Self::PartitionTask(pt) => pt.resource_request(),
        }
    }

    pub fn task_id(&self) -> usize {
        match self {
            Self::ScanTask(pt) => pt.task_id(),
            Self::PartitionTask(pt) => pt.task_id(),
        }
    }

    pub fn task_op_name(&self) -> &str {
        match self {
            Self::ScanTask(pt) => pt.task_op.name(),
            Self::PartitionTask(pt) => pt.task_op.name(),
        }
    }
}

#[derive(Debug)]
pub struct PartitionTask<V: VirtualPartition> {
    inputs: Vec<V>,
    task_op: Arc<dyn PartitionTaskOp<Input = V::TaskOpInput>>,
    resource_request: ResourceRequest,
    // partial_metadata: PartitionMetadata,
    task_id: usize,
}

impl<V: VirtualPartition> PartitionTask<V> {
    pub fn new(inputs: Vec<V>, task_op: Arc<dyn PartitionTaskOp<Input = V::TaskOpInput>>) -> Self {
        let task_id = TASK_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let input_metadata = inputs.iter().map(|vp| vp.metadata()).collect::<Vec<_>>();
        task_op.with_input_metadata(&input_metadata);
        let resource_request = task_op.resource_request_with_input_metadata(&input_metadata);
        // let partial_metadata = task_op.partial_metadata_from_input_metadata(&input_metadata);
        Self {
            inputs,
            task_op,
            resource_request,
            // partial_metadata,
            task_id,
        }
    }

    pub fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    pub fn partial_metadata(&self) -> &PartitionMetadata {
        todo!()
        // &self.partial_metadata
    }

    pub fn task_id(&self) -> usize {
        self.task_id
    }

    pub fn into_executable(
        self,
    ) -> (
        Vec<V>,
        Arc<dyn PartitionTaskOp<Input = V::TaskOpInput>>,
        ResourceRequest,
    ) {
        (self.inputs, self.task_op, self.resource_request)
    }
}
