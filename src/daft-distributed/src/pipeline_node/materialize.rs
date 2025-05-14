use common_error::DaftResult;
use futures::Stream;

use crate::{
    pipeline_node::PipelineOutput,
    scheduling::{dispatcher::TaskDispatcherHandle, task::Task},
};

#[allow(dead_code)]
pub(crate) fn materialize_all_pipeline_outputs<T: Task>(
    _input: impl Stream<Item = DaftResult<PipelineOutput>> + Send + Unpin + 'static,
    _task_dispatcher_handle: TaskDispatcherHandle<T>,
) {
    todo!("FLOTILLA_MS1: Implement materialize_all_pipeline_outputs")
}

#[allow(dead_code)]
pub(crate) fn materialize_running_pipeline_outputs(
    _input: impl Stream<Item = DaftResult<PipelineOutput>> + Send + Unpin + 'static,
) {
    todo!("FLOTILLA_MS1: Implement materialize_running_pipeline_outputs")
}
