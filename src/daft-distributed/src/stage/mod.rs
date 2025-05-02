use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::LogicalPlanRef;
use futures::{Stream, StreamExt};

use crate::{
    channel::{create_channel, Receiver},
    pipeline_node::{self, PipelineOutput, RunningPipelineNode},
    runtime::JoinSet,
    scheduling::{
        dispatcher::{SubmittedTask, TaskDispatcher, TaskDispatcherHandle},
        worker::WorkerManagerFactory,
    },
};

pub(crate) struct Stage {
    logical_plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
}

impl Stage {
    fn new(logical_plan: LogicalPlanRef, config: Arc<DaftExecutionConfig>) -> Self {
        Self {
            logical_plan,
            config,
        }
    }
}

impl Stage {
    pub(crate) fn run_stage(
        self,
        psets: HashMap<String, Vec<PartitionRef>>,
        worker_manager_factory: Box<dyn WorkerManagerFactory>,
    ) -> DaftResult<RunningStage> {
        let mut pipeline_node =
            pipeline_node::logical_plan_to_pipeline_node(self.logical_plan, self.config, psets)?;
        let mut stage_context = StageContext::try_new(worker_manager_factory)?;
        let running_node = pipeline_node.start(&mut stage_context);
        let materialized_results_receiver =
            materialize_stage_results(running_node, &mut stage_context)?;
        let running_stage = RunningStage::new(materialized_results_receiver, stage_context);
        Ok(running_stage)
    }
}

fn materialize_stage_results(
    running_node: RunningPipelineNode,
    stage_context: &mut StageContext,
) -> DaftResult<Receiver<PartitionRef>> {
    // This task is responsible for submitting any unsubmitted tasks
    fn spawn_task_finalizer(
        joinset: &mut JoinSet<DaftResult<()>>,
        mut running_node: RunningPipelineNode,
        task_dispatcher_handle: TaskDispatcherHandle,
    ) -> DaftResult<Receiver<FinalizedTask>> {
        let (tx, rx) = create_channel(1);
        joinset.spawn(async move {
            while let Some(pipeline_result) = running_node.next().await {
                let pipeline_output = pipeline_result?;
                let to_send = match pipeline_output {
                    // If the pipeline output is a materialized partition, we can just send it through the channel
                    PipelineOutput::Materialized(partition) => {
                        FinalizedTask::Materialized(partition)
                    }
                    // If the pipeline output is a task, we need to submit it to the task dispatcher
                    PipelineOutput::Task(task) => {
                        let task_result_handle = task_dispatcher_handle.submit_task(task).await?;
                        FinalizedTask::Running(task_result_handle)
                    }
                    // If the task is already running, we can just send it through the channel
                    PipelineOutput::Running(submitted_task) => {
                        FinalizedTask::Running(submitted_task)
                    }
                };
                if tx.send(to_send).await.is_err() {
                    break;
                }
            }
            Ok(())
        });
        Ok(rx)
    }

    // This task is responsible for materializing the results of submitted tasks
    fn spawn_task_materializer(
        joinset: &mut JoinSet<DaftResult<()>>,
        mut finalized_tasks_receiver: Receiver<FinalizedTask>,
    ) -> DaftResult<Receiver<PartitionRef>> {
        let (tx, rx) = create_channel(1);
        joinset.spawn(async move {
            while let Some(finalized_task) = finalized_tasks_receiver.recv().await {
                match finalized_task {
                    // If the pipeline output is a materialized partition, we can just send it through the channel
                    FinalizedTask::Materialized(partition) => {
                        if tx.send(partition).await.is_err() {
                            break;
                        }
                    }
                    FinalizedTask::Running(submitted_task) => {
                        if let Some(result) = submitted_task.await {
                            if tx.send(result?).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
            Ok(())
        });
        Ok(rx)
    }

    enum FinalizedTask {
        Materialized(PartitionRef),
        Running(SubmittedTask),
    }

    let task_dispatcher_handle = stage_context.get_task_dispatcher_handle();
    let finalized_tasks_receiver = spawn_task_finalizer(
        &mut stage_context.joinset,
        running_node,
        task_dispatcher_handle,
    )?;
    let materialized_results_receiver =
        spawn_task_materializer(&mut stage_context.joinset, finalized_tasks_receiver)?;
    Ok(materialized_results_receiver)
}

enum RunningStageState {
    // Running: Stage is running and we are waiting for the result from the receiver
    Running(Receiver<PartitionRef>, Option<StageContext>),
    // Finishing: No more results will be produced, and we are waiting for the joinset to finish
    Finishing(JoinSet<DaftResult<()>>),
    // Finished: No more results will be produced, and the joinset has finished
    Finished,
}

#[allow(dead_code)]
pub(crate) struct RunningStage {
    running_stage_state: RunningStageState,
}

impl RunningStage {
    fn new(result_materializer: Receiver<PartitionRef>, stage_context: StageContext) -> Self {
        Self {
            running_stage_state: RunningStageState::Running(
                result_materializer,
                Some(stage_context),
            ),
        }
    }
}

impl Stream for RunningStage {
    type Item = DaftResult<PartitionRef>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        fn poll_inner(
            state: &mut RunningStageState,
            cx: &mut Context<'_>,
        ) -> Option<Poll<Option<DaftResult<PartitionRef>>>> {
            match state {
                // Running: Stage is running and we are waiting for the result from the receiver
                RunningStageState::Running(result_receiver, stage_context) => {
                    match result_receiver.poll_recv(cx) {
                        // Received a result from the receiver
                        Poll::Ready(Some(result)) => Some(Poll::Ready(Some(Ok(result)))),
                        // No more results will be produced, transition to finishing state where we wait for the joinset to finish
                        Poll::Ready(None) => {
                            let (task_dispatcher_handle, joinset) = stage_context
                                .take()
                                .expect("StageContext should exist")
                                .into_inner();
                            // No more tasks to dispatch, drop the handle
                            drop(task_dispatcher_handle);
                            *state = RunningStageState::Finishing(joinset);
                            None
                        }
                        // Still waiting for a result from the receiver
                        Poll::Pending => Some(Poll::Pending),
                    }
                }
                // Finishing: No more results will be produced, and we are waiting for the joinset to finish
                RunningStageState::Finishing(joinset) => match joinset.poll_join_next(cx) {
                    // Received a result from the joinset
                    Poll::Ready(Some(result)) => match result {
                        Ok(Ok(())) => Some(Poll::Ready(None)),
                        Ok(Err(e)) => Some(Poll::Ready(Some(Err(e)))),
                        Err(e) => Some(Poll::Ready(Some(Err(DaftError::External(e.into()))))),
                    },
                    // Joinset is empty, transition to finished state
                    Poll::Ready(None) => {
                        *state = RunningStageState::Finished;
                        None
                    }
                    // Still waiting for a result from the joinset
                    Poll::Pending => Some(Poll::Pending),
                },
                // Finished: No more results will be produced, and the joinset has finished
                RunningStageState::Finished => Some(Poll::Ready(None)),
            }
        }

        loop {
            if let Some(poll) = poll_inner(&mut self.running_stage_state, cx) {
                return poll;
            }
        }
    }
}

pub(crate) struct StageContext {
    task_dispatcher_handle: TaskDispatcherHandle,
    joinset: JoinSet<DaftResult<()>>,
}

impl StageContext {
    fn try_new(worker_manager_factory: Box<dyn WorkerManagerFactory>) -> DaftResult<Self> {
        let worker_manager = worker_manager_factory.create_worker_manager()?;
        let task_dispatcher = TaskDispatcher::new(worker_manager);
        let mut joinset = JoinSet::new();
        let task_dispatcher_handle =
            TaskDispatcher::spawn_task_dispatcher(task_dispatcher, &mut joinset);
        Ok(Self {
            task_dispatcher_handle,
            joinset,
        })
    }

    pub fn get_task_dispatcher_handle(&self) -> TaskDispatcherHandle {
        self.task_dispatcher_handle.clone()
    }

    pub fn spawn_task_on_joinset(
        &mut self,
        f: impl Future<Output = DaftResult<()>> + Send + 'static,
    ) {
        self.joinset.spawn(f);
    }

    pub fn into_inner(self) -> (TaskDispatcherHandle, JoinSet<DaftResult<()>>) {
        (self.task_dispatcher_handle, self.joinset)
    }
}

#[allow(dead_code)]
pub(crate) fn split_at_stage_boundary(
    plan: &LogicalPlanRef,
    config: &Arc<DaftExecutionConfig>,
) -> DaftResult<(Stage, Option<LogicalPlanRef>)> {
    struct StageBoundarySplitter {
        next_stage: Option<Stage>,
        _config: Arc<DaftExecutionConfig>,
    }

    impl TreeNodeRewriter for StageBoundarySplitter {
        type Node = LogicalPlanRef;

        fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }

        fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            // TODO: Implement stage boundary splitting. Stage boundaries will be defined by the presence of a repartition, or the root of the plan.
            // If it is the root of the plan, we will return a collect stage.
            // If it is a repartition, we will return a shuffle map stage.
            Ok(Transformed::no(node))
        }
    }

    let mut splitter = StageBoundarySplitter {
        next_stage: None,
        _config: config.clone(),
    };

    let transformed = plan.clone().rewrite(&mut splitter)?;

    if let Some(next_stage) = splitter.next_stage {
        Ok((next_stage, Some(transformed.data)))
    } else {
        // make collect stage
        let plan = transformed.data;
        let stage = Stage::new(plan, config.clone());
        Ok((stage, None))
    }
}
