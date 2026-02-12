use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::OrderingAwareJoinSet;
use daft_micropartition::MicroPartition;

use crate::{
    channel::Receiver,
    pipeline_message::{InputId, PipelineMessage},
};

/// Events yielded by the `next_event()` function.
pub(crate) enum PipelineEvent<TaskResult> {
    /// A spawned task completed with this result.
    TaskCompleted(TaskResult),
    /// A morsel arrived from the input channel.
    Morsel {
        input_id: InputId,
        partition: Arc<MicroPartition>,
    },
    /// A flush message arrived for this input_id.
    Flush(InputId),
    /// The input channel closed (all senders dropped).
    InputClosed,
}

/// Yield the next event from either the task set or the input receiver.
///
/// Returns `Some(event)` if an event was produced, or `None` when both
/// branches are disabled (no tasks running and input is closed).
///
/// This is one iteration of the biased `tokio::select!` loop.
pub(crate) async fn next_event<TaskResult: Send + 'static>(
    task_set: &mut OrderingAwareJoinSet<DaftResult<TaskResult>>,
    max_concurrency: usize,
    receiver: &mut Receiver<PipelineMessage>,
    input_closed: &mut bool,
) -> DaftResult<PipelineEvent<TaskResult>> {
    tokio::select! {
        // Branch 1: Receive input (only if we can spawn task and receiver open)
        msg = receiver.recv(), if task_set.len() < max_concurrency && !*input_closed => {
            match msg {
                Some(PipelineMessage::Morsel { input_id, partition }) => {
                    Ok(PipelineEvent::Morsel { input_id, partition })
                }
                Some(PipelineMessage::Flush(input_id)) => {
                    Ok(PipelineEvent::Flush(input_id))
                }
                None => {
                    *input_closed = true;
                    Ok(PipelineEvent::InputClosed)
                }
            }
        }
        // Branch 2: Join completed task (only if tasks exist)
        Some(task_result) = task_set.join_next(), if !task_set.is_empty() => {
            match task_result {
                Ok(Ok(result)) => Ok(PipelineEvent::TaskCompleted(result)),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(e.into()),
            }
        }
    }
}
