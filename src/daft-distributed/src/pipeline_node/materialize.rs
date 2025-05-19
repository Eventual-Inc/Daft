use common_error::DaftResult;
use common_partitioning::PartitionRef;
use futures::Stream;

use crate::{
    pipeline_node::PipelineOutput,
    scheduling::{scheduler::SchedulerHandle, task::Task},
};

#[allow(dead_code)]
pub(crate) fn materialize_all_pipeline_outputs<T: Task>(
    _input: impl Stream<Item = DaftResult<PipelineOutput>> + Send + Unpin + 'static,
    _scheduler_handle: SchedulerHandle<T>,
) -> impl Stream<Item = DaftResult<PartitionRef>> + Send + Unpin + 'static {
    futures::stream::empty()
}

#[allow(dead_code)]
pub(crate) fn materialize_running_pipeline_outputs(
    _input: impl Stream<Item = DaftResult<PipelineOutput>> + Send + Unpin + 'static,
) -> impl Stream<Item = DaftResult<PipelineOutput>> + Send + Unpin + 'static {
    futures::stream::empty()
}
