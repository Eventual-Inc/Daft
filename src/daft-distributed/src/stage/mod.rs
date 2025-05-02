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
    channel::{create_channel, Receiver, Sender},
    pipeline_node::{self, PipelineOutput, RunningPipelineNode},
    runtime::JoinSet,
    scheduling::{
        dispatcher::{TaskDispatcher, TaskDispatcherHandle},
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
    let (result_sender, result_receiver) = create_channel(1);
    async fn materialize_stage_results(
        result_sender: Sender<PartitionRef>,
        mut running_node: RunningPipelineNode,
        task_dispatcher_handle: TaskDispatcherHandle,
    ) -> DaftResult<()> {
        let mut tasks_to_await = Vec::new();
        while let Some(result) = running_node.next().await {
            let result = result?;
            match result {
                PipelineOutput::Materialized(partition) => {
                    if result_sender.send(partition).await.is_err() {
                        break;
                    }
                }
                PipelineOutput::Task(task) => {
                    let task_result_handle = task_dispatcher_handle.submit_task(task).await?;
                    tasks_to_await.push(task_result_handle);
                }
                PipelineOutput::Running(running) => {
                    tasks_to_await.push(running);
                }
            }
        }
        for task_result_handle in tasks_to_await {
            if let Some(result) = task_result_handle.await {
                if result_sender.send(result?).await.is_err() {
                    break;
                }
            }
        }
        Ok(())
    }
    let task_dispatcher_handle = stage_context.get_task_dispatcher_handle();
    stage_context.spawn_task_on_joinset(materialize_stage_results(
        result_sender,
        running_node,
        task_dispatcher_handle,
    ));
    Ok(result_receiver)
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
