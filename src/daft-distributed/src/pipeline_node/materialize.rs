use std::sync::atomic::{AtomicUsize, Ordering};

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use futures::{Stream, StreamExt};

use crate::{
    pipeline_node::PipelineOutput,
    scheduling::{
        dispatcher::{SubmittedTask, TaskDispatcherHandle},
        task::{SwordfishTask, Task},
    },
    utils::{
        channel::{create_channel, Receiver, Sender},
        joinset::{JoinSet, OrderedJoinSet},
        stream::JoinableForwardingStream,
    },
};

pub(crate) fn materialize_all_pipeline_outputs<T: Task>(
    input: impl Stream<Item = DaftResult<PipelineOutput<T>>> + Send + Unpin + 'static,
    task_dispatcher_handle: TaskDispatcherHandle<T>,
    mut joinset: JoinSet<DaftResult<()>>,
) -> impl Stream<Item = DaftResult<PartitionRef>> {
    enum FinalizedTask {
        Materialized(PartitionRef),
        Running(SubmittedTask),
    }

    async fn task_finalizer<T: Task>(
        mut input: impl Stream<Item = DaftResult<PipelineOutput<T>>> + Unpin,
        tx: Sender<FinalizedTask>,
        task_dispatcher_handle: TaskDispatcherHandle<T>,
    ) -> DaftResult<()> {
        while let Some(pipeline_result) = input.next().await {
            let pipeline_output = pipeline_result?;
            let finalized_tasks = match pipeline_output {
                // If the pipeline output is a materialized partition, we can just send it through the channel
                PipelineOutput::Materialized(partition) => {
                    vec![FinalizedTask::Materialized(partition)]
                }
                // If the pipeline output is a task, we need to submit it to the task dispatcher
                PipelineOutput::Tasks(tasks) => {
                    let submitted_tasks = task_dispatcher_handle.submit_many_tasks(tasks).await?;
                    submitted_tasks
                        .into_iter()
                        .map(FinalizedTask::Running)
                        .collect()
                }
                // If the task is already running, we can just send it through the channel
                PipelineOutput::Running(submitted_task) => {
                    vec![FinalizedTask::Running(submitted_task)]
                }
            };
            for finalized_task in finalized_tasks {
                if tx.send(finalized_task).await.is_err() {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn task_materializer(
        mut finalized_tasks_receiver: Receiver<FinalizedTask>,
        tx: Sender<PartitionRef>,
        max_concurrent_tasks: AtomicUsize,
    ) -> DaftResult<()> {
        let mut pending_tasks: OrderedJoinSet<Option<DaftResult<Vec<PartitionRef>>>> =
            OrderedJoinSet::new();
        loop {
            let num_pending = pending_tasks.num_pending();
            let max_concurrent_tasks = max_concurrent_tasks.load(Ordering::Relaxed);

            tokio::select! {
                Some(finalized_task) = finalized_tasks_receiver.recv(), if num_pending < max_concurrent_tasks => {
                    match finalized_task {
                        FinalizedTask::Materialized(partition) => {
                            pending_tasks.spawn(async move { Some(Ok(vec![partition])) });
                        }
                        FinalizedTask::Running(submitted_task) => {
                            pending_tasks.spawn(submitted_task);
                        }
                    }
                }
                Some(result) = pending_tasks.join_next(), if num_pending > 0 => {
                    let result = result?;
                    if let Some(result) = result {
                        let result = result?;
                        for partition in result {
                            if tx.send(partition).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }

        Ok(())
    }

    let (finalized_tasks_sender, finalized_tasks_receiver) = create_channel(1);
    let (materialized_results_sender, materialized_results_receiver) = create_channel(1);

    let max_concurrent_tasks = AtomicUsize::new(100);

    joinset.spawn(task_finalizer(
        input,
        finalized_tasks_sender,
        task_dispatcher_handle,
    ));
    joinset.spawn(task_materializer(
        finalized_tasks_receiver,
        materialized_results_sender,
        max_concurrent_tasks,
    ));

    let materialized_result_stream =
        tokio_stream::wrappers::ReceiverStream::new(materialized_results_receiver);
    JoinableForwardingStream::new(materialized_result_stream, joinset)
}

// This function is responsible for awaiting the results of any running tasks
pub(crate) fn materialize_running_pipeline_outputs<T: Task>(
    input: impl Stream<Item = DaftResult<PipelineOutput<T>>> + Send + Unpin + 'static,
    mut joinset: JoinSet<DaftResult<()>>,
) -> impl Stream<Item = DaftResult<PipelineOutput<T>>> {
    async fn result_awaiter<T: Task>(
        mut pipeline_output_stream: impl Stream<Item = DaftResult<PipelineOutput<T>>>
            + Send
            + Unpin
            + 'static,
        tx: Sender<PipelineOutput<T>>,
        max_concurrent_tasks: AtomicUsize,
    ) -> DaftResult<()> {
        let mut pending_tasks: OrderedJoinSet<Option<DaftResult<Vec<PartitionRef>>>> =
            OrderedJoinSet::new();
        loop {
            let num_pending = pending_tasks.num_pending();
            let max_concurrent_tasks = max_concurrent_tasks.load(Ordering::Relaxed);

            tokio::select! {
                Some(pipeline_output) = pipeline_output_stream.next(), if num_pending < max_concurrent_tasks => {
                    let pipeline_output = pipeline_output?;
                    match pipeline_output {
                        PipelineOutput::Materialized(partition) => {
                            if tx.send(PipelineOutput::Materialized(partition)).await.is_err() {
                                break;
                            }
                        }
                        PipelineOutput::Tasks(tasks) => {
                            if tx.send(PipelineOutput::Tasks(tasks)).await.is_err() {
                                break;
                            }
                        }
                        PipelineOutput::Running(submitted_task) => {
                            pending_tasks.spawn(submitted_task);
                        }
                    }
                }
                Some(result) = pending_tasks.join_next(), if num_pending > 0 => {
                    let result = result?;
                    if let Some(result) = result {
                        let result = result?;
                        for partition in result {
                            if tx.send(PipelineOutput::Materialized(partition)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }

        Ok(())
    }

    let (tx, rx) = create_channel(1);

    joinset.spawn(result_awaiter(input, tx, AtomicUsize::new(100)));
    let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    JoinableForwardingStream::new(output_stream, joinset)
}
