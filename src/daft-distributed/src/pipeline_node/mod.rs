use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_logical_plan::LogicalPlanRef;
use futures::{Stream, StreamExt};
use materialize::materialize_all_pipeline_outputs;

use crate::{
    scheduling::{
        scheduler::{SchedulerHandle, SubmittedTask},
        task::SwordfishTask,
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

/// The materialized output of a completed pipeline node.
/// Contains both the partition data as well as metadata about the partition.
/// Right now, the only metadata is the worker id that has it so we can try
/// to schedule follow-up pipeline nodes on the same worker.
#[derive(Debug)]
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
pub(crate) enum PipelineOutput {
    Materialized(MaterializedOutput),
    Task(SwordfishTask),
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
    result_receiver: Receiver<PipelineOutput>,
}

impl RunningPipelineNode {
    #[allow(dead_code)]
    fn new(result_receiver: Receiver<PipelineOutput>) -> Self {
        Self { result_receiver }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Receiver<PipelineOutput> {
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

    pub fn into_stream(self) -> impl Stream<Item = PipelineOutput> + Send + Unpin + 'static {
        ReceiverStream::new(self.result_receiver)
    }
}

#[allow(dead_code)]
pub(crate) fn logical_plan_to_pipeline_node(
    _plan: LogicalPlanRef,
    _config: Arc<DaftExecutionConfig>,
    _psets: HashMap<String, Vec<PartitionRef>>,
) -> DaftResult<Box<dyn DistributedPipelineNode>> {
    todo!("FLOTILLA_MS1: Implement translation of logical plan to pipeline nodes")
}
