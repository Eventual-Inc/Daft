use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_metrics::QueryID;
use common_partitioning::PartitionRef;
use daft_logical_plan::LogicalPlanRef;
use futures::{Stream, StreamExt};

use super::{DistributedPhysicalPlan, QueryIdx, PlanResult};
use crate::{
    pipeline_node::{
        MaterializedOutput, SubmittableTaskStream, logical_plan_to_pipeline_node,
        materialize::materialize_all_pipeline_outputs,
    },
    scheduling::{
        scheduler::{SchedulerHandle, spawn_scheduler_actor},
        task::{SwordfishTask, TaskID},
        worker::{Worker, WorkerManager},
    },
    statistics::{StatisticsManagerRef},
    utils::{
        channel::{Sender, create_channel},
        joinset::{JoinSet, create_join_set},
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
    scheduler_handle: SchedulerHandle<SwordfishTask>,
    joinset: JoinSet<DaftResult<()>>,
    task_id_counter: TaskIDCounter,
}

impl PlanExecutionContext {
    fn new(scheduler_handle: SchedulerHandle<SwordfishTask>) -> Self {
        let joinset = JoinSet::new();
        Self {
            scheduler_handle,
            joinset,
            task_id_counter: TaskIDCounter::new(),
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
}

#[derive(Clone)]
pub(crate) struct QueryConfig {
    pub query_id: QueryID,
    pub query_idx: QueryIdx,
    pub config: Arc<DaftExecutionConfig>,
}

impl QueryConfig {
    pub fn new(query_id: QueryID, query_idx: QueryIdx, config: Arc<DaftExecutionConfig>) -> Self {
        Self { query_id, query_idx, config }
    }
}

pub(crate) struct RunningPlan {
    task_stream: SubmittableTaskStream,
    plan_context: PlanExecutionContext,
}

impl RunningPlan {
    fn new(task_stream: SubmittableTaskStream, plan_context: PlanExecutionContext) -> Self {
        Self {
            task_stream,
            plan_context,
        }
    }

    pub fn materialize(
        self,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin + 'static {
        let joinset = self.plan_context.joinset;
        materialize_all_pipeline_outputs(self.task_stream, scheduler_handle, Some(joinset))
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

    async fn execute_plan(
        &self,
        query_config: QueryConfig,
        logical_plan: LogicalPlanRef,
        psets: HashMap<String, Vec<PartitionRef>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        sender: Sender<MaterializedOutput>,
        statistics_manager: StatisticsManagerRef,
    ) -> DaftResult<()> {
        let mut plan_context = PlanExecutionContext::new(scheduler_handle.clone());
        let query_id = query_config.query_id.clone();

        let head_pipeline_node =
            logical_plan_to_pipeline_node(query_config, logical_plan.clone(), Arc::new(psets))?;

        statistics_manager.handle_start(query_id.clone())?;
        let head_running_node = head_pipeline_node.produce_tasks(&mut plan_context);
        let running_stage = RunningPlan::new(head_running_node, plan_context);

        let mut materialized_result_stream = running_stage.materialize(scheduler_handle);
        while let Some(result) = materialized_result_stream.next().await {
            if sender.send(result?).await.is_err() {
                break;
            }
        }
        statistics_manager.handle_finish(query_id).await?;
        Ok(())
    }

    pub fn run_plan(
        self: &Arc<Self>,
        plan: &DistributedPhysicalPlan,
        psets: HashMap<String, Vec<PartitionRef>>,
        statistics_manager: StatisticsManagerRef,
    ) -> DaftResult<PlanResult> {
        let query_id = plan.query_id();
        let query_idx = plan.query_idx();
        let config = plan.execution_config().clone();
        let logical_plan = plan.logical_plan().clone();
        let plan_config = QueryConfig::new(query_id, query_idx, config);

        let runtime = get_or_init_runtime();
        let (result_sender, result_receiver) = create_channel(1);

        let this = self.clone();

        let joinset = runtime.block_on_current_thread(async move {
            let mut joinset = create_join_set();
            let scheduler_handle = spawn_scheduler_actor(
                self.worker_manager.clone(),
                &mut joinset,
                statistics_manager.clone(),
            );

            joinset.spawn(async move {
                this.execute_plan(
                    plan_config,
                    logical_plan,
                    psets,
                    scheduler_handle,
                    result_sender,
                    statistics_manager,
                )
                .await
            });
            joinset
        });
        Ok(PlanResult::new(joinset, result_receiver))
    }
}
