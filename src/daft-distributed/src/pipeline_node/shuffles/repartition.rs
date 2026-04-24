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

pub(crate) struct RepartitionWriteNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    repartition_spec: RepartitionSpec,
    shuffle_backend: ShuffleBackend,
    num_partitions: usize,
    child: DistributedPipelineNode,
}

impl RepartitionWriteNode {
    const NODE_NAME: &'static str = "RepartitionWrite";

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
            NodeCategory::Intermediate,
        );
        let config = PipelineNodeConfig::new(
            schema.clone(),
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context: context.clone(),
            repartition_spec,
            shuffle_backend: ShuffleBackend::new(&context, schema, backend),
            num_partitions,
            child,
        }
    }
}

impl PipelineNodeImpl for RepartitionWriteNode {
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
        self.shuffle_backend.register_cleanup(plan_context);

        let schema = self.shuffle_backend.schema().clone();
        let node_id = self.shuffle_backend.node_id();
        let local_shuffle_backend = self.shuffle_backend.local_shuffle_backend();
        let num_partitions = self.num_partitions;
        let repartition_spec = self.repartition_spec.clone();
        input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::repartition_write(
                input,
                num_partitions,
                schema.clone(),
                local_shuffle_backend.clone(),
                repartition_spec.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            )
        })
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let backend_name = match self.shuffle_backend.backend() {
            DistributedShuffleBackend::Ray => "RayShuffleWrite",
            DistributedShuffleBackend::Flight(_) => "FlightShuffleWrite",
        };
        let mut res = vec![format!(
            "{backend_name}: {}",
            self.repartition_spec.var_name()
        )];
        res.extend(self.repartition_spec.multiline_display());
        res
    }
}

pub(crate) struct RepartitionNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    shuffle_backend: ShuffleBackend,
    num_partitions: usize,
    repartition_spec: RepartitionSpec,
    child: DistributedPipelineNode,
}

impl RepartitionNode {
    const NODE_NAME: &'static str = "Repartition";

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

        Self {
            config,
            context: context.clone(),
            shuffle_backend: ShuffleBackend::new(&context, schema, backend),
            num_partitions,
            repartition_spec,
            child,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        write_stream: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let outputs = write_stream.materialize(
            scheduler_handle.clone(),
            self.context.query_idx,
            task_id_counter,
        );

        let transposed_outputs =
            transpose_materialized_outputs_from_stream(outputs, self.num_partitions).await?;

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
        let write_stream = self.child.clone().produce_tasks(plan_context);

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();

        let execution = async move {
            self.execution_loop(write_stream, task_id_counter, result_tx, scheduler_handle)
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

    fn shuffle_info(&self) -> Option<serde_json::Value> {
        let strategy = self.repartition_spec.var_name();
        let backend = match self.shuffle_backend.backend() {
            DistributedShuffleBackend::Ray => "Ray",
            DistributedShuffleBackend::Flight(_) => "Flight",
        };
        Some(serde_json::json!({
            "strategy": strategy,
            "num_partitions": self.num_partitions,
            "backend": backend,
        }))
    }
}
