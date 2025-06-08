use common_error::DaftResult;
use common_partitioning::PartitionRef;
use futures::{Stream, StreamExt};
use materialize::{materialize_all_pipeline_outputs, materialize_running_pipeline_outputs};

use crate::{
    scheduling::{
        scheduler::{SchedulerHandle, SubmittedTask},
        task::{SwordfishTask, Task},
        worker::WorkerId,
    },
    stage::StageContext,
    utils::channel::{Receiver, ReceiverStream},
};

mod in_memory_source;
mod intermediate;
mod limit;
pub(crate) mod materialize;
mod scan_source;
mod translate;

pub(crate) use translate::logical_plan_to_pipeline_node;

/// The materialized output of a completed pipeline node.
/// Contains both the partition data as well as metadata about the partition.
/// Right now, the only metadata is the worker id that has it so we can try
/// to schedule follow-up pipeline nodes on the same worker.
#[derive(Clone, Debug)]
pub(crate) struct MaterializedOutput {
    partition: PartitionRef,
    worker_id: WorkerId,
}

impl MaterializedOutput {
    #[allow(dead_code)]
    pub fn new(partition: PartitionRef, worker_id: WorkerId) -> Self {
        Self {
            partition,
            worker_id,
        }
    }

    pub fn partition(&self) -> &PartitionRef {
        &self.partition
    }

    #[allow(dead_code)]
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn into_inner(self) -> (PartitionRef, WorkerId) {
        (self.partition, self.worker_id)
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum PipelineOutput<T: Task> {
    Materialized(MaterializedOutput),
    Task(T),
    Running(SubmittedTask),
}

pub(crate) trait DistributedPipelineNode: Send + Sync {
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
    #[allow(dead_code)]
    fn children(&self) -> Vec<&dyn DistributedPipelineNode>;
    #[allow(dead_code)]
    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode;
}

#[allow(dead_code)]
pub(crate) struct RunningPipelineNode {
    result_receiver: Receiver<PipelineOutput<SwordfishTask>>,
}

impl RunningPipelineNode {
    #[allow(dead_code)]
    fn new(result_receiver: Receiver<PipelineOutput<SwordfishTask>>) -> Self {
        Self { result_receiver }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Receiver<PipelineOutput<SwordfishTask>> {
        self.result_receiver
    }

    #[allow(dead_code)]
    pub fn materialize(
        self,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin + 'static {
        let stream = self.into_stream().map(Ok);
        materialize_all_pipeline_outputs(stream, scheduler_handle)
    }

    pub fn materialize_running(
        self,
    ) -> impl Stream<Item = DaftResult<PipelineOutput<SwordfishTask>>> + Send + Unpin + 'static
    {
        let stream = self.into_stream().map(Ok);
        materialize_running_pipeline_outputs(stream)
    }

    pub fn into_stream(
        self,
    ) -> impl Stream<Item = PipelineOutput<SwordfishTask>> + Send + Unpin + 'static {
        ReceiverStream::new(self.result_receiver)
    }
}
