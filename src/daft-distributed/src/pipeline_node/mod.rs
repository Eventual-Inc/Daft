use std::{
    collections::HashMap,
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use futures::Stream;
use materialize::materialize_all_pipeline_outputs;

use crate::{
    scheduling::{
        dispatcher::{SubmittedTask, TaskDispatcherHandle, TaskDispatcherHandleRef},
        task::{SwordfishTask, Task},
        worker::WorkerId,
    },
    stage::StageContext,
    utils::channel::Receiver,
};

mod in_memory_source;
mod intermediate;
mod limit;
pub(crate) mod materialize;
mod scan_source;
mod translate;

pub(crate) use translate::logical_plan_to_pipeline_node;

pub(crate) trait DistributedPipelineNode: Send + Sync + Debug {
    fn as_tree_display(&self) -> &dyn TreeDisplay;
    fn name(&self) -> &'static str;
    fn children(&self) -> Vec<&dyn DistributedPipelineNode>;
    fn start(
        &self,
        stage_context: &mut StageContext,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> RunningPipelineNode<SwordfishTask>;
}

#[allow(dead_code)]
pub(crate) struct RunningPipelineNode<T: Task> {
    result_receiver: Receiver<PipelineOutput<T>>,
}

impl<T: Task> RunningPipelineNode<T> {
    #[allow(dead_code)]
    fn new(result_receiver: Receiver<PipelineOutput<T>>) -> Self {
        Self { result_receiver }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Receiver<PipelineOutput<T>> {
        self.result_receiver
    }

    pub fn materialize(
        self,
        task_dispatcher_handle: TaskDispatcherHandleRef<T>,
    ) -> impl Stream<Item = DaftResult<MaterializedOutput>> {
        materialize_all_pipeline_outputs(self, task_dispatcher_handle)
    }
}

// instead of directly implementing Stream, we implement an into_stream method that returns an impl stream
impl<T: Task> Stream for RunningPipelineNode<T> {
    type Item = DaftResult<PipelineOutput<T>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.result_receiver.poll_recv(cx) {
            Poll::Ready(Some(result)) => Poll::Ready(Some(Ok(result))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MaterializedOutput {
    partition: PartitionRef,
    worker_id: WorkerId,
}

impl MaterializedOutput {
    pub fn new(partition: PartitionRef, worker_id: WorkerId) -> Self {
        Self {
            partition,
            worker_id,
        }
    }

    pub fn partition(&self) -> &PartitionRef {
        &self.partition
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn into_inner(self) -> (PartitionRef, WorkerId) {
        (self.partition, self.worker_id)
    }
}

#[derive(Debug)]
pub(crate) enum PipelineOutput<T: Task> {
    Materialized(MaterializedOutput),
    Task(T),
    Running(SubmittedTask),
}
