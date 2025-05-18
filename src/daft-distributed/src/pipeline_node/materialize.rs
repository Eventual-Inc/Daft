use common_error::DaftResult;
use common_partitioning::PartitionRef;
use futures::Stream;

use crate::{pipeline_node::PipelineOutput, scheduling::dispatcher::TaskDispatcherHandle};

#[allow(dead_code)]
pub(crate) fn materialize_all_pipeline_outputs(
    _input: impl Stream<Item = DaftResult<PipelineOutput>> + Send + Unpin + 'static,
    _task_dispatcher_handle: TaskDispatcherHandle,
) -> impl Stream<Item = DaftResult<PartitionRef>> + Send + Unpin + 'static {
    futures::stream::empty()
}

#[allow(dead_code)]
pub(crate) fn materialize_running_pipeline_outputs(
    _input: impl Stream<Item = DaftResult<PipelineOutput>> + Send + Unpin + 'static,
) -> impl Stream<Item = DaftResult<PipelineOutput>> + Send + Unpin + 'static {
    futures::stream::empty()
}
