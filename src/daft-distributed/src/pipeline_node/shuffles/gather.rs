use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_local_plan::{GatherWriteBackend, LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::partitioning::UnknownClusteringConfig;
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl,
        TaskBuilderStream,
        shuffles::{
            backends::{DistributedShuffleBackend, ShuffleBackend, ShuffleBackendReadSpec},
            partition_groups::{
                flight_server_cache_mapping_from_outputs, ray_partition_groups_from_outputs,
            },
        },
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::channel::{Sender, create_channel},
};

pub(crate) struct GatherNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    shuffle_backend: ShuffleBackend,
    child: DistributedPipelineNode,
}

impl GatherNode {
    const NODE_NAME: &'static str = "Gather";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        schema: SchemaRef,
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
            Arc::new(UnknownClusteringConfig::new(1).into()),
        );
        Self {
            config,
            context: context.clone(),
            shuffle_backend: ShuffleBackend::new(&context, schema, 1, backend),
            child,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        local_gather_write_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let outputs = local_gather_write_node
            .task_outputs(
                scheduler_handle.clone(),
                self.context.query_idx,
                task_id_counter,
            )
            .try_collect::<Vec<_>>()
            .await?;

        let read_spec = match self.shuffle_backend.backend() {
            DistributedShuffleBackend::Ray => ShuffleBackendReadSpec::Ray {
                partition_groups: ray_partition_groups_from_outputs(outputs, 1)?,
            },
            DistributedShuffleBackend::Flight(_) => ShuffleBackendReadSpec::Flight {
                server_cache_mapping: flight_server_cache_mapping_from_outputs(outputs)?,
            },
        };

        self.shuffle_backend
            .emit_read_tasks(read_spec, self.as_ref(), result_tx)
            .await
    }
}

impl PipelineNodeImpl for GatherNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let backend_name = match self.shuffle_backend.backend() {
            DistributedShuffleBackend::Ray => "RayShuffle",
            DistributedShuffleBackend::Flight(_) => "FlightShuffle",
        };
        vec![format!("{backend_name}: Gather")]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let self_arc = self.clone();
        self.shuffle_backend.register_cleanup(plan_context);
        let schema = self.config.schema.clone();
        let node_id = self.node_id();
        let local_gather_write_node = input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::gather_write(
                input,
                schema.clone(),
                match self_arc.shuffle_backend.backend() {
                    DistributedShuffleBackend::Ray => GatherWriteBackend::Ray,
                    DistributedShuffleBackend::Flight(backend) => GatherWriteBackend::Flight {
                        shuffle_id: backend.shuffle_id,
                        shuffle_dirs: backend.shuffle_dirs.clone(),
                        compression: backend.compression.clone(),
                    },
                },
                daft_logical_plan::stats::StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            )
        });

        let (result_tx, result_rx) = create_channel(1);
        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();

        let execution = async move {
            self.execution_loop(
                local_gather_write_node,
                task_id_counter,
                result_tx,
                scheduler_handle,
            )
            .await
        };

        plan_context.spawn(execution);
        TaskBuilderStream::from(result_rx)
    }
}
