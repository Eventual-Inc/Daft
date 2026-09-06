use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
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
        scheduler::{SchedulerHandle, WorkerSnapshot, spawn_scheduler_actor},
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
    /// Trees on a cluster-shared mount: one copy exists for the whole cluster, so
    /// these are removed once rather than by every node.
    shared_shuffle_dirs: Vec<String>,
    /// Shuffles whose Flight registrations the workers should forget once those
    /// trees are gone. Tracked separately from the directories because the
    /// registrations live in worker memory, not on any of these paths.
    shuffle_ids: Vec<u64>,
    /// Output partition counts of this plan's shuffles, for the width check in
    /// [`PlanRunner::warn_if_narrower_than_cluster`].
    shuffle_widths: Vec<usize>,
    statistics_manager: StatisticsManagerRef,
}

impl PlanExecutionContext {
    pub(crate) fn new(
        query_idx: QueryIdx,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        statistics_manager: StatisticsManagerRef,
    ) -> Self {
        let joinset = JoinSet::new();
        Self {
            query_idx,
            scheduler_handle,
            joinset,
            task_id_counter: TaskIDCounter::new(),
            shuffle_dirs: Vec::new(),
            shared_shuffle_dirs: Vec::new(),
            shuffle_ids: Vec::new(),
            shuffle_widths: Vec::new(),
            statistics_manager,
        }
    }

    pub fn scheduler_handle(&self) -> SchedulerHandle<SwordfishTask> {
        self.scheduler_handle.clone()
    }

    pub fn statistics_manager(&self) -> &StatisticsManagerRef {
        &self.statistics_manager
    }

    pub fn spawn(&mut self, task: impl Future<Output = DaftResult<()>> + Send + 'static) {
        self.joinset.spawn(task);
    }

    pub fn task_id_counter(&self) -> TaskIDCounter {
        self.task_id_counter.clone()
    }

    /// Register node-local shuffle directories for cleanup when the plan completes
    pub fn register_shuffle_dirs(&mut self, dirs: Vec<String>) {
        self.shuffle_dirs.extend(dirs);
    }

    /// Register shared-mount shuffle directories for cleanup when the plan completes
    pub fn register_shared_shuffle_dirs(&mut self, dirs: Vec<String>) {
        self.shared_shuffle_dirs.extend(dirs);
    }

    /// Register a shuffle whose worker-side Flight registrations should be dropped
    /// when the plan completes
    pub fn register_shuffle_id(&mut self, shuffle_id: u64) {
        self.shuffle_ids.push(shuffle_id);
    }

    /// Register how many output partitions a shuffle produces, which is how many
    /// tasks its reduce side can run at once
    pub fn register_shuffle_width(&mut self, num_partitions: usize) {
        self.shuffle_widths.push(num_partitions);
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
                statistics_manager.clone(),
            );

            joinset.spawn(async move {
                this.run_plan_impl(
                    pipeline_node,
                    query_idx,
                    scheduler_handle,
                    statistics_manager,
                    result_sender,
                )
                .await
            });
            joinset
        });
        Ok(PlanResult::new(joinset, result_receiver))
    }

    /// Point out when a shuffle is too narrow to occupy the cluster it is running
    /// on — and say what to do about it, because the obvious move is wrong.
    ///
    /// A shuffle's reduce side runs one task per output partition, so a plan whose
    /// narrowest shuffle has N partitions cannot keep more than N cores busy no
    /// matter how many are available. Measured: a 1 TiB shuffle into 4096
    /// partitions on 20,224 cores ran with roughly a fifth of the cluster doing
    /// anything.
    ///
    /// The trap is that raising the partition count to match the cluster makes it
    /// worse, not better. The same query with 20,224 input partitions instead of
    /// 4096 did not finish in three hours — at least 4.8x slower — because it cut
    /// each (map, partition) cell from ~59 KiB to ~12 KiB while leaving the
    /// per-read overhead unchanged, and that overhead lands on the reduce side,
    /// which is ~98% of the query. Partition counts follow from bytes per
    /// partition; cluster width should follow from the partition count.
    fn warn_if_narrower_than_cluster(&self, shuffle_widths: &[usize]) {
        // The narrowest shuffle is the one that caps the plan.
        let Some(&narrowest) = shuffle_widths.iter().min() else {
            return;
        };
        let Ok(snapshots) = self.worker_manager.worker_snapshots() else {
            return;
        };
        let total_cpus = snapshots
            .iter()
            .map(WorkerSnapshot::total_num_cpus)
            .sum::<f64>() as usize;

        // Only worth saying when a large part of a large cluster is idle: half the
        // cores unusable, and enough of them in absolute terms to matter.
        let idle = total_cpus.saturating_sub(narrowest);
        if narrowest == 0 || narrowest * 2 > total_cpus || idle < 64 {
            return;
        }
        tracing::warn!(
            "This plan's narrowest shuffle has {} output partitions but the cluster has {} CPUs, \
             so at most {} of them can be busy during its reduce stage. Note that raising the \
             partition count is usually the wrong fix: it shrinks each partition's share of every \
             map file, and the extra per-read overhead lands on the reduce side, which dominates \
             runtime. Size partitions by bytes, and size the cluster to the partition count.",
            narrowest,
            total_cpus,
            narrowest,
        );
    }

    async fn run_plan_impl(
        &self,
        pipeline_node: DistributedPipelineNode,
        query_idx: QueryIdx,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        statistics_manager: StatisticsManagerRef,
        sender: Sender<MaterializedOutput>,
    ) -> DaftResult<()> {
        let mut plan_context =
            PlanExecutionContext::new(query_idx, scheduler_handle.clone(), statistics_manager);

        let running_node = pipeline_node.produce_tasks(&mut plan_context);
        let shuffle_dirs = std::mem::take(&mut plan_context.shuffle_dirs);
        let shared_shuffle_dirs = std::mem::take(&mut plan_context.shared_shuffle_dirs);
        let shuffle_ids = std::mem::take(&mut plan_context.shuffle_ids);
        let shuffle_widths = std::mem::take(&mut plan_context.shuffle_widths);
        self.warn_if_narrower_than_cluster(&shuffle_widths);
        let running_stage = RunningPlan::new(running_node, plan_context);

        let mut materialized_result_stream = running_stage.materialize(scheduler_handle);
        // Held rather than propagated with `?`: a failed plan is exactly the one
        // whose shuffle output most needs removing, and returning here would skip
        // the cleanup below — leaving the trees on every node's disk and on the
        // shared mount, and the registrations in every worker's memory, for the
        // lifetime of the cluster.
        let mut plan_result = Ok(());
        while let Some(result) = materialized_result_stream.next().await {
            match result {
                Ok(output) => {
                    if sender.send(output).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    plan_result = Err(e);
                    break;
                }
            }
        }
        // The stream owns the plan's `JoinSet`, so dropping it stops the
        // coordinator dispatching further tasks before their output directories
        // are deleted.
        drop(materialized_result_stream);

        if (!shuffle_dirs.is_empty() || !shared_shuffle_dirs.is_empty() || !shuffle_ids.is_empty())
            && let Err(e) = self
                .worker_manager
                .cleanup_shuffles(shuffle_dirs, shared_shuffle_dirs, shuffle_ids)
                .await
        {
            tracing::warn!("Failed to clean up after flight shuffles: {}", e);
        }

        plan_result
    }
}
