use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::UnknownClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
        shuffles::backends::{DistributedShuffleBackend, ShuffleBackend},
    },
    plan::{PlanConfig, PlanExecutionContext, TaskSubmissionContext},
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
            NodeType::Gather,
            NodeCategory::BlockingSink,
        );
        let config = PipelineNodeConfig::new(
            schema.clone(),
            plan_config.config.clone(),
            Arc::new(UnknownClusteringConfig::new(1).into()),
        );
        let shuffle_backend = ShuffleBackend::new(&context, schema, backend);
        Self {
            config,
            context,
            shuffle_backend,
            child,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        local_gather_write_node: TaskBuilderStream,
        submission_ctx: TaskSubmissionContext,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Drive all upstream gather-write tasks to completion and collect their outputs.
        let materialized = local_gather_write_node
            .materialize(scheduler_handle.clone(), submission_ctx)
            .try_collect::<Vec<MaterializedOutput>>()
            .await?;

        // Gather = single read task that consumes every ref from every worker.
        self.shuffle_backend
            .emit_read_tasks(vec![materialized], self.as_ref(), result_tx)
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
            DistributedShuffleBackend::Ray => "RayGather",
            DistributedShuffleBackend::Flight(_) => "FlightGather",
        };
        vec![backend_name.to_string()]
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
        let local_gather_write_node = input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::gather_write(
                input,
                schema.clone(),
                local_shuffle_backend.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            )
        });

        let (result_tx, result_rx) = create_channel(1);

        let self_arc = self.clone();
        let submission_ctx = plan_context.task_submission_context();
        let scheduler_handle = plan_context.scheduler_handle();

        let execution = async move {
            self_arc
                .execution_loop(
                    local_gather_write_node,
                    submission_ctx,
                    result_tx,
                    scheduler_handle,
                )
                .await
        };

        plan_context.spawn(execution);
        TaskBuilderStream::from(result_rx)
    }
}
