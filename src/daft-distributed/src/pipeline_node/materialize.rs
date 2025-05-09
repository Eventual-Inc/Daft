use common_error::DaftResult;
use common_partitioning::PartitionRef;
use futures::StreamExt;

use super::RunningPipelineNode;
use crate::{
    channel::{create_channel, Receiver, Sender},
    pipeline_node::PipelineOutput,
    runtime::OrderedJoinSet,
    scheduling::dispatcher::{SubmittedTask, TaskDispatcherHandle},
    stage::StageContext,
};

enum FinalizedTask {
    Materialized(PartitionRef),
    Running(SubmittedTask),
}

async fn task_finalizer(
    mut running_node: RunningPipelineNode,
    tx: Sender<FinalizedTask>,
    task_dispatcher_handle: TaskDispatcherHandle,
) -> DaftResult<()> {
    while let Some(pipeline_result) = running_node.next().await {
        let pipeline_output = pipeline_result?;
        let to_send = match pipeline_output {
            // If the pipeline output is a materialized partition, we can just send it through the channel
            PipelineOutput::Materialized(partition) => FinalizedTask::Materialized(partition),
            // If the pipeline output is a task, we need to submit it to the task dispatcher
            PipelineOutput::Task(task) => {
                let task_result_handle = task_dispatcher_handle.submit_task(task).await?;
                FinalizedTask::Running(task_result_handle)
            }
            // If the task is already running, we can just send it through the channel
            PipelineOutput::Running(submitted_task) => FinalizedTask::Running(submitted_task),
        };
        if tx.send(to_send).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn task_materializer(
    mut finalized_tasks_receiver: Receiver<FinalizedTask>,
    tx: Sender<PartitionRef>,
) -> DaftResult<()> {
    fn handle_finalized_task(
        finalized_task: FinalizedTask,
        pending_tasks: &mut OrderedJoinSet<Option<DaftResult<PartitionRef>>>,
    ) {
        match finalized_task {
            FinalizedTask::Materialized(partition) => {
                pending_tasks.spawn(async move { Some(Ok(partition)) });
            }
            FinalizedTask::Running(submitted_task) => {
                pending_tasks.spawn(submitted_task);
            }
        }
    }

    async fn handle_materialized_result(
        materialized_result: DaftResult<Option<DaftResult<PartitionRef>>>,
        tx: &Sender<PartitionRef>,
    ) -> DaftResult<bool> {
        let mut should_break = false;
        let maybe_result = materialized_result?;
        if let Some(result) = maybe_result {
            if tx.send(result?).await.is_err() {
                should_break = true;
            }
        }
        Ok(should_break)
    }

    let mut pending_tasks = OrderedJoinSet::new();
    loop {
        let num_pending = pending_tasks.num_pending();
        tokio::select! {
            Some(finalized_task) = finalized_tasks_receiver.recv() => {
                handle_finalized_task(finalized_task, &mut pending_tasks);
            }
            Some(result) = pending_tasks.join_next(), if num_pending > 0 => {
                if handle_materialized_result(result, &tx).await? {
                    break;
                }
            }
            else => break,
        }
    }

    Ok(())
}

pub(crate) fn materialize_pipeline_results(
    running_node: RunningPipelineNode,
    stage_context: &mut StageContext,
) -> Receiver<PartitionRef> {
    // This task is responsible for submitting any unsubmitted tasks
    fn spawn_task_finalizer(
        stage_context: &mut StageContext,
        running_node: RunningPipelineNode,
        task_dispatcher_handle: TaskDispatcherHandle,
    ) -> Receiver<FinalizedTask> {
        let (tx, rx) = create_channel(1);
        stage_context.spawn_task_on_joinset(task_finalizer(
            running_node,
            tx,
            task_dispatcher_handle,
        ));
        rx
    }

    // This task is responsible for materializing the results of finalized tasks
    fn spawn_task_materializer(
        stage_context: &mut StageContext,
        finalized_tasks_receiver: Receiver<FinalizedTask>,
    ) -> Receiver<PartitionRef> {
        let (tx, rx) = create_channel(1);
        stage_context.spawn_task_on_joinset(task_materializer(finalized_tasks_receiver, tx));
        rx
    }

    let task_dispatcher_handle = stage_context.get_task_dispatcher_handle();
    let finalized_tasks_receiver =
        spawn_task_finalizer(stage_context, running_node, task_dispatcher_handle);
    let materialized_results_receiver =
        spawn_task_materializer(stage_context, finalized_tasks_receiver);
    materialized_results_receiver
}
