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
use common_treenode::{TreeNode, TreeNodeRecursion};
use futures::{Stream, StreamExt};

use super::{DistributedPhysicalPlan, PlanResult, QueryIdx};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, SubmittableTaskStream,
        logical_plan_to_pipeline_node, materialize::materialize_all_pipeline_outputs,
    },
    scheduling::{
        scheduler::{SchedulerHandle, spawn_scheduler_actor},
        task::{SwordfishTask, TaskID},
        worker::{Worker, WorkerManager},
    },
    statistics::{StatisticsManager, StatisticsSubscriber},
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
pub(crate) struct PlanConfig {
    pub query_idx: QueryIdx,
    pub query_id: QueryID,
    pub config: Arc<DaftExecutionConfig>,
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
        pipeline_node: DistributedPipelineNode,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        sender: Sender<MaterializedOutput>,
    ) -> DaftResult<()> {
        let mut plan_context = PlanExecutionContext::new(scheduler_handle.clone());

        let running_node = pipeline_node.produce_tasks(&mut plan_context);
        let running_stage = RunningPlan::new(running_node, plan_context);

        let mut materialized_result_stream = running_stage.materialize(scheduler_handle);
        while let Some(result) = materialized_result_stream.next().await {
            if sender.send(result?).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    pub fn run_plan(
        self: &Arc<Self>,
        plan: &DistributedPhysicalPlan,
        psets: HashMap<String, Vec<PartitionRef>>,
        subscribers: Vec<Box<dyn StatisticsSubscriber>>,
    ) -> DaftResult<PlanResult> {
        let query_idx = plan.idx();
        let query_id = plan.query_id();
        let config = plan.execution_config().clone();
        let logical_plan = plan.logical_plan().clone();
        let plan_config = PlanConfig::new(query_idx, query_id, config);

        let pipeline_node =
            logical_plan_to_pipeline_node(plan_config, logical_plan, Arc::new(psets))?;

        // Extract runtime stats from pipeline nodes to create the StatisticsManager
        let mut runtime_stats = HashMap::new();
        pipeline_node.apply(|node| {
            runtime_stats.insert(node.node_id(), node.runtime_stats());
            Ok(TreeNodeRecursion::Continue)
        })?;

        let statistics_manager = StatisticsManager::new(runtime_stats, subscribers);

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
                this.execute_plan(pipeline_node, scheduler_handle, result_sender)
                    .await
            });
            joinset
        });
        Ok(PlanResult::new(joinset, result_receiver))
    }
}
