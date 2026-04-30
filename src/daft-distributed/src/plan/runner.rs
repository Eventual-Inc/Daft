use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use common_daft_config::DaftExecutionConfig;
use daft_common_error::DaftResult;
use common_metrics::QueryID;
use common_runtime::{JoinSet, create_join_set};
use futures::{Stream, StreamExt};

use super::{PlanResult, QueryIdx};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, TaskBuilderStream,
        materialize::materialize_all_pipeline_outputs,
    },
    plan::DistributedPhysicalPlan,
    scheduling::{
        scheduler::{SchedulerHandle, spawn_scheduler_actor},
        task::{SwordfishTask, TaskID},
        worker::{Worker, WorkerManager},
    },
    statistics::StatisticsManagerRef,
    utils::{
        channel::{Sender, create_channel},
        runtime::get_or_init_runtime,
    },
};

#[derive(Clone)]
pub(crate) struct TaskIDCounter {
    counter: Arc<AtomicU32>,
}

impl TaskIDCounter {
    pub fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn next(&self) -> TaskID {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}

pub(crate) struct PlanExecutionContext {
    query_idx: QueryIdx,
    scheduler_handle: SchedulerHandle<SwordfishTask>,
    joinset: JoinSet<DaftResult<()>>,
    task_id_counter: TaskIDCounter,
    shuffle_dirs: Vec<String>,
}

impl PlanExecutionContext {
    pub(crate) fn new(
        query_idx: QueryIdx,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> Self {
        let joinset = JoinSet::new();
        Self {
            query_idx,
            scheduler_handle,
            joinset,
            task_id_counter: TaskIDCounter::new(),
            shuffle_dirs: Vec::new(),
        }
    }

    pub fn scheduler_handle(&self) -> SchedulerHandle<SwordfishTask> {
        self.scheduler_handle.clone()
    }

    pub fn spawn(&mut self, task: impl Future<Output = DaftResult<()>> + Send + 'static) {
        self.joinset.spawn(task);
    }

    pub fn task_id_counter(&self) -> TaskIDCounter {
        self.task_id_counter.clone()
    }

    /// Register shuffle directories for cleanup when the plan completes
    pub fn register_shuffle_dirs(&mut self, dirs: Vec<String>) {
        self.shuffle_dirs.extend(dirs);
    }
}

#[derive(Clone)]
pub(crate) struct PlanConfig {
    pub query_idx: QueryIdx,
    pub query_id: QueryID,
    pub config: Arc<DaftExecutionConfig>,
}

impl From<&DistributedPhysicalPlan> for PlanConfig {
    fn from(plan: &DistributedPhysicalPlan) -> Self {
        Self {
            query_idx: plan.idx(),
            query_id: plan.query_id(),
            config: plan.execution_config().clone(),
        }
    }
}

impl PlanConfig {
    pub fn new(query_idx: QueryIdx, query_id: QueryID, config: Arc<DaftExecutionConfig>) -> Self {
        Self {
            query_idx,
            query_id,
            config,
        }
    }
}

pub(crate) struct RunningPlan {
    task_stream: TaskBuilderStream,
    plan_context: PlanExecutionContext,
}

impl RunningPlan {
    pub(crate) fn new(task_stream: TaskBuilderStream, plan_context: PlanExecutionContext) -> Self {
        Self {
            task_stream,
            plan_context,
        }
    }

    pub fn materialize(
        self,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin + 'static {
        let task_id_counter = self.plan_context.task_id_counter();
        let joinset = self.plan_context.joinset;
        let stream = self
            .task_stream
            .map(move |builder| builder.build(self.plan_context.query_idx, &task_id_counter));
        materialize_all_pipeline_outputs(stream, scheduler_handle, Some(joinset))
    }
}

#[derive(Clone)]
pub(crate) struct PlanRunner<W: Worker<Task = SwordfishTask>> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
}

impl<W: Worker<Task = SwordfishTask>> PlanRunner<W> {
    pub fn new(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self { worker_manager }
    }

    pub fn run_plan(
        self: &Arc<Self>,
        query_idx: QueryIdx,
        pipeline_node: DistributedPipelineNode,
        statistics_manager: StatisticsManagerRef,
    ) -> DaftResult<PlanResult> {
        let runtime = get_or_init_runtime();
        let (result_sender, result_receiver) = create_channel(1);
        let this = self.clone();
        let joinset = runtime.block_on_current_thread(async move {
            let mut joinset = create_join_set();
            let scheduler_handle = spawn_scheduler_actor(
                self.worker_manager.clone(),
                &mut joinset,
                statistics_manager,
            );

            joinset.spawn(async move {
                this.run_plan_impl(pipeline_node, query_idx, scheduler_handle, result_sender)
                    .await
            });
            joinset
        });
        Ok(PlanResult::new(joinset, result_receiver))
    }

    async fn run_plan_impl(
        &self,
        pipeline_node: DistributedPipelineNode,
        query_idx: QueryIdx,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        sender: Sender<MaterializedOutput>,
    ) -> DaftResult<()> {
        let mut plan_context = PlanExecutionContext::new(query_idx, scheduler_handle.clone());

        let running_node = pipeline_node.produce_tasks(&mut plan_context);
        let shuffle_dirs = std::mem::take(&mut plan_context.shuffle_dirs);
        let running_stage = RunningPlan::new(running_node, plan_context);

        let mut materialized_result_stream = running_stage.materialize(scheduler_handle);
        while let Some(result) = materialized_result_stream.next().await {
            if sender.send(result?).await.is_err() {
                break;
            }
        }

        // Clean up shuffle directories via Ray remote functions
        #[cfg(feature = "python")]
        if !shuffle_dirs.is_empty()
            && let Err(e) = crate::python::ray::clear_shuffle_dirs_on_all_nodes(shuffle_dirs).await
        {
            tracing::warn!("Failed to clear flight shuffle directories: {}", e);
        }

        Ok(())
    }
}
