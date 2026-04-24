use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::{
    Meter, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::StatSnapshotImpl as _,
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::UnknownClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, PipelineNodeConfig,
        PipelineNodeContext,
        shuffles::backends::{DistributedShuffleBackend, ShuffleBackend},
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::{
        RuntimeStats,
        stats::{BaseCounters, RuntimeStatsRef},
    },
    utils::channel::{Sender, create_channel},
};

const INITIAL_BATCH_PHASE: &str = "local";
const REBATCH_PHASE: &str = "rebatch";

pub struct IntoBatchesStats {
    base: BaseCounters,
}

impl IntoBatchesStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        Self {
            base: BaseCounters::new(meter, context),
        }
    }
}

impl RuntimeStats for IntoBatchesStats {
    fn handle_worker_node_stats(&self, node_info: &NodeInfo, snapshot: &StatSnapshot) {
        self.base.add_duration_us(snapshot.duration_us());
        if let StatSnapshot::Default(snapshot) = snapshot
            && let Some(phase) = &node_info.node_phase
        {
            // Track input rows/bytes for the initial local batching pass and output rows/bytes for the rebatch pass.
            if phase == INITIAL_BATCH_PHASE {
                self.base.add_rows_in(snapshot.rows_in);
                self.base.add_bytes_in(snapshot.bytes_in);
            } else if phase == REBATCH_PHASE {
                self.base.add_rows_out(snapshot.rows_out);
                self.base.add_bytes_out(snapshot.bytes_out);
            }
        }
    }

    fn export_snapshot(&self) -> StatSnapshot {
        self.base.export_default_snapshot()
    }

    fn increment_num_tasks(&self) {
        self.base.increment_num_tasks();
    }
}

pub(crate) struct IntoBatchesNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    batch_size: usize,
    shuffle_backend: ShuffleBackend,
    child: DistributedPipelineNode,
}

// The threshold at which we will emit a batch of data to the next task.
// For instance, if the batch size is 100 and the threshold is 0.8, we will emit a batch
// of data to the next task once we have 80 rows of data.
// This is a heuristic to avoid creating batches that are too big. For instance, if we had
// materialized outputs from two partitions that of size 80, we would emit two batches of size 80
// instead of one batch of size 160.
const BATCH_SIZE_THRESHOLD: f64 = 0.8;

impl IntoBatchesNode {
    const NODE_NAME: &'static str = "IntoBatches";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        batch_size: usize,
        schema: SchemaRef,
        backend: DistributedShuffleBackend,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::IntoBatches,
            NodeCategory::StreamingSink,
        );
        let config = PipelineNodeConfig::new(
            schema.clone(),
            plan_config.config.clone(),
            Arc::new(
                UnknownClusteringConfig::new(
                    child.config().clustering_spec.num_partitions().max(2),
                )
                .into(),
            ),
        );
        let shuffle_backend = ShuffleBackend::new(&context, schema, backend);
        Self {
            config,
            context,
            batch_size,
            shuffle_backend,
            child,
        }
    }

    /// Build the rebatch task for a group of materialized upstream outputs.
    ///
    /// Read side is backend-dispatched by `build_refs_task_builder`
    /// (`shuffle_read(Ray) + with_psets` or `shuffle_read(Flight) +
    /// with_flight_shuffle_reads`). The task's output is terminated with
    /// `gather_write(backend)` so refs produced by this node always match the
    /// configured backend — never a mix of Ray and Flight.
    fn build_rebatch_task(
        &self,
        group: Vec<MaterializedOutput>,
        group_size: usize,
    ) -> SwordfishTaskBuilder {
        let node_id = self.node_id();
        let refs = group
            .into_iter()
            .flat_map(|output| output.into_inner().0)
            .collect::<Vec<_>>();

        let schema = self.shuffle_backend.schema().clone();
        let local_shuffle_backend = self.shuffle_backend.local_shuffle_backend();

        self.shuffle_backend
            .build_refs_task_builder(refs, self, move |read_plan| {
                let rebatch = LocalPhysicalPlan::into_batches(
                    read_plan,
                    group_size,
                    true,
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(node_id as usize)).with_phase(REBATCH_PHASE),
                );
                LocalPhysicalPlan::gather_write(
                    rebatch,
                    schema,
                    local_shuffle_backend,
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(node_id as usize)),
                )
            })
    }

    async fn execute_into_batches(
        self: Arc<Self>,
        input_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let mut materialized_stream =
            input_node.materialize(scheduler_handle, self.context.query_idx, task_id_counter);

        let mut current_group: Vec<MaterializedOutput> = Vec::new();
        let mut current_group_size = 0;

        while let Some(mat) = materialized_stream.next().await {
            for mat in mat?.split_into_materialized_outputs() {
                let rows = mat.num_rows();
                if rows == 0 {
                    continue;
                }

                current_group.push(mat);
                current_group_size += rows;
                if current_group_size >= (self.batch_size as f64 * BATCH_SIZE_THRESHOLD) as usize {
                    let group_size = std::mem::take(&mut current_group_size);
                    let materialized_outputs = std::mem::take(&mut current_group);
                    let builder = self.build_rebatch_task(materialized_outputs, group_size);
                    if result_tx.send(builder).await.is_err() {
                        break;
                    }
                }
            }
        }

        if !current_group.is_empty() {
            let builder = self.build_rebatch_task(current_group, current_group_size);
            let _ = result_tx.send(builder).await;
        }
        Ok(())
    }
}

impl PipelineNodeImpl for IntoBatchesNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(IntoBatchesStats::new(meter, self.context()))
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let backend_name = match self.shuffle_backend.backend() {
            DistributedShuffleBackend::Ray => "Ray",
            DistributedShuffleBackend::Flight(_) => "Flight",
        };
        vec![format!(
            "IntoBatches({}): {}",
            backend_name, self.batch_size
        )]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        self.shuffle_backend.register_cleanup(plan_context);
        let input_node = self.child.clone().produce_tasks(plan_context);
        let node_id = self.node_id();
        let batch_size = self.batch_size;
        let schema = self.shuffle_backend.schema().clone();
        let local_shuffle_backend = self.shuffle_backend.local_shuffle_backend();
        // Phase 1: initial per-task batching, then `gather_write(backend)` so
        // each task's output is a single ref matching the configured backend.
        // Keeping the ref type consistent here means phase-2 tasks always
        // consume refs of the configured backend type — no Ray/Flight mix.
        let local_into_batches_node = input_node.pipeline_instruction(self.clone(), move |input| {
            let batched = LocalPhysicalPlan::into_batches(
                input,
                batch_size,
                false, // No need strict batch sizes for the child tasks, as we coalesce them later on.
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)).with_phase(INITIAL_BATCH_PHASE),
            );
            LocalPhysicalPlan::gather_write(
                batched,
                schema.clone(),
                local_shuffle_backend.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            )
        });

        let (result_tx, result_rx) = create_channel(1);
        let execution_future = self.execute_into_batches(
            local_into_batches_node,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        );
        plan_context.spawn(execution_future);

        TaskBuilderStream::from(result_rx)
    }
}
