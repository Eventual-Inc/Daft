use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl,
        TaskBuilderStream,
        shuffles::backends::{DistributedShuffleBackend, ShuffleBackend},
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::{
        channel::{Sender, create_channel},
        transpose::transpose_materialized_outputs_from_stream,
    },
};

pub(crate) struct RepartitionNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    repartition_spec: RepartitionSpec,
    shuffle_backend: ShuffleBackend,
    num_partitions: usize,
    seal_policy: String,
    seal_partition_threshold: usize,
    /// True when this node is running the new Flight server-side repartition
    /// path: map tasks emit `ShuffleWrite` instead of `RepartitionWrite`, and
    /// the orchestrator skips its explicit seal call (the Flight server flushes
    /// residuals lazily on the first read).
    use_server_side_repartition: bool,
    /// Per-task speculative-buffer threshold passed into the `ShuffleWrite`
    /// local op. Ignored on the legacy path.
    mode_switch_threshold_bytes: usize,
    /// Server-side raw accumulator threshold, passed through to the flight
    /// server via `register_shuffle_spec`. Ignored on the legacy path.
    server_repartition_threshold_bytes: usize,
    child: DistributedPipelineNode,
}

impl RepartitionNode {
    const NODE_NAME: &'static str = "Repartition";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        num_partitions: usize,
        backend: DistributedShuffleBackend,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::Repartition,
            NodeCategory::BlockingSink,
        );
        let config = PipelineNodeConfig::new(
            schema.clone(),
            plan_config.config.clone(),
            repartition_spec
                .to_clustering_spec(child.config().clustering_spec.num_partitions())
                .into(),
        );

        let use_server_side_repartition = matches!(&backend, DistributedShuffleBackend::Flight(_))
            && plan_config.config.flight_shuffle_server_side_repartition
            && matches!(
                &repartition_spec,
                RepartitionSpec::Hash(_) | RepartitionSpec::Random(_)
            );
        Self {
            config,
            context: context.clone(),
            repartition_spec,
            shuffle_backend: ShuffleBackend::new(&context, schema, backend),
            num_partitions,
            seal_policy: plan_config.config.flight_shuffle_seal.clone(),
            seal_partition_threshold: plan_config.config.flight_shuffle_seal_partition_threshold,
            use_server_side_repartition,
            mode_switch_threshold_bytes: plan_config
                .config
                .flight_shuffle_mode_switch_threshold_bytes,
            server_repartition_threshold_bytes: plan_config
                .config
                .flight_shuffle_server_repartition_threshold_bytes,
            child,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        local_shuffle_write_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let outputs = local_shuffle_write_node.materialize(
            scheduler_handle.clone(),
            self.context.query_idx,
            task_id_counter,
        );

        let transposed_outputs =
            transpose_materialized_outputs_from_stream(outputs, self.num_partitions).await?;

        if !self.use_server_side_repartition {
            // Legacy path: ask every participating shuffle server to consolidate
            // its per-task entry groups into one file per partition before any
            // read task fires. No-op on the Ray backend; gated by partition-count
            // threshold on the Flight backend.
            self.shuffle_backend
                .seal(
                    &transposed_outputs,
                    &self.seal_policy,
                    self.seal_partition_threshold,
                    self.num_partitions,
                )
                .await?;
        }
        // New path (use_server_side_repartition=true): no explicit seal. The
        // Flight server flushes any residual raw entries on the first read.

        self.shuffle_backend
            .emit_read_tasks(transposed_outputs, self.as_ref(), result_tx)
            .await
    }
}

impl PipelineNodeImpl for RepartitionNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let self_arc = self.clone();
        self.shuffle_backend.register_cleanup(plan_context);

        let schema = self.shuffle_backend.schema().clone();
        let node_id = self.shuffle_backend.node_id();
        let local_shuffle_backend = self.shuffle_backend.local_shuffle_backend();
        let num_partitions = self.num_partitions;
        let repartition_spec = self.repartition_spec.clone();
        let use_server_side = self.use_server_side_repartition;
        let mode_switch_threshold = self.mode_switch_threshold_bytes;
        let server_repartition_threshold = self.server_repartition_threshold_bytes;
        let local_shuffle_write_node =
            input_node.pipeline_instruction(self.clone(), move |input| {
                if use_server_side {
                    // Pull the Flight config out of the local-plan backend enum;
                    // use_server_side_repartition is only set when backend is
                    // Flight (see RepartitionNode::new), so this match is exhaustive
                    // in practice and panics here would be a planner bug.
                    let (shuffle_id, shuffle_dirs, compression) = match &local_shuffle_backend {
                        daft_local_plan::ShuffleBackend::Flight {
                            shuffle_id,
                            shuffle_dirs,
                            compression,
                        } => (*shuffle_id, shuffle_dirs.clone(), compression.clone()),
                        daft_local_plan::ShuffleBackend::Ray => {
                            panic!(
                                "use_server_side_repartition is only valid for the Flight backend"
                            )
                        }
                    };
                    LocalPhysicalPlan::shuffle_write(
                        input,
                        num_partitions,
                        schema.clone(),
                        shuffle_id,
                        shuffle_dirs,
                        compression,
                        repartition_spec.clone(),
                        mode_switch_threshold,
                        server_repartition_threshold,
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(node_id as usize)),
                    )
                } else {
                    LocalPhysicalPlan::repartition_write(
                        input,
                        num_partitions,
                        schema.clone(),
                        local_shuffle_backend.clone(),
                        repartition_spec.clone(),
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(node_id as usize)),
                    )
                }
            });

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();

        let execution = async move {
            self_arc
                .execution_loop(
                    local_shuffle_write_node,
                    task_id_counter,
                    result_tx,
                    scheduler_handle,
                )
                .await
        };

        plan_context.spawn(execution);
        TaskBuilderStream::from(result_rx)
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let backend_name = match self.shuffle_backend.backend() {
            DistributedShuffleBackend::Ray => "RayShuffle",
            DistributedShuffleBackend::Flight(_) => "FlightShuffle",
        };
        let mut res = vec![format!(
            "{backend_name}: {}",
            self.repartition_spec.var_name()
        )];
        res.extend(self.repartition_spec.multiline_display());
        res
    }
}
