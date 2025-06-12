use common_display::{
    ascii::fmt_tree_gitstyle,
    mermaid::{MermaidDisplayVisitor, SubgraphOptions},
    tree::TreeDisplay,
    DisplayLevel,
};
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use futures::{Stream, StreamExt};
use materialize::{materialize_all_pipeline_outputs, materialize_running_pipeline_outputs};

use crate::{
    plan::PlanID,
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask, SubmittedTask},
        task::{SwordfishTask, Task},
        worker::WorkerId,
    },
    stage::{StageContext, StageID},
    utils::channel::{Receiver, ReceiverStream},
};

#[cfg(feature = "python")]
mod actor_udf;
mod in_memory_source;
mod intermediate;
mod limit;
pub(crate) mod materialize;
mod scan_source;
mod translate;

pub(crate) use translate::logical_plan_to_pipeline_node;
pub(crate) type NodeID = usize;

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
    Task(SubmittableTask<T>),
    Running(SubmittedTask),
}

#[allow(dead_code)]
pub(crate) trait DistributedPipelineNode: Send + Sync {
    fn name(&self) -> &'static str;
    fn children(&self) -> Vec<&dyn DistributedPipelineNode>;
    fn start(&self, stage_context: &mut StageContext) -> RunningPipelineNode;
    fn plan_id(&self) -> &PlanID;
    fn stage_id(&self) -> &StageID;
    fn node_id(&self) -> &NodeID;
    fn as_tree_display(&self) -> &dyn TreeDisplay;
}

/// Visualize a distributed pipeline as Mermaid markdown
pub fn viz_distributed_pipeline_mermaid(
    root: &dyn DistributedPipelineNode,
    display_type: DisplayLevel,
    bottom_up: bool,
    subgraph_options: Option<SubgraphOptions>,
) -> String {
    let mut output = String::new();
    let mut visitor =
        MermaidDisplayVisitor::new(&mut output, display_type, bottom_up, subgraph_options);
    visitor.fmt(root.as_tree_display()).unwrap();
    output
}

/// Visualize a distributed pipeline as ASCII text
pub fn viz_distributed_pipeline_ascii(root: &dyn DistributedPipelineNode, simple: bool) -> String {
    let mut s = String::new();
    let level = if simple {
        DisplayLevel::Compact
    } else {
        DisplayLevel::Default
    };
    fmt_tree_gitstyle(root.as_tree_display(), 0, &mut s, level).unwrap();
    s
}

#[allow(dead_code)]
#[derive(Debug)]
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
