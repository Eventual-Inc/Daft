use std::sync::Arc;

use common_error::DaftResult;
use daft_logical_plan::partitioning::UnknownClusteringConfig;
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        PipelineNodeImpl, SubmittableTaskStream, make_in_memory_task_from_materialized_outputs,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    utils::channel::{Sender, create_channel},
};

pub(crate) struct GatherNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    child: DistributedPipelineNode,
}

impl GatherNode {
    const NODE_NAME: NodeName = "Gather";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            Arc::new(UnknownClusteringConfig::new(1).into()),
        );
        Self {
            config,
            context,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    // Async execution to get all partitions out
    async fn execution_loop(
        self: Arc<Self>,
        input_node: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Trigger materialization of all inputs
        let materialized = input_node
            .materialize(scheduler_handle.clone())
            .try_collect::<Vec<_>>()
            .await?;

        let self_clone = self.clone();
        let task = make_in_memory_task_from_materialized_outputs(
            TaskContext::from((&self_clone.context, task_id_counter.next())),
            materialized,
            self_clone.config.schema.clone(),
            &(self_clone as Arc<dyn PipelineNodeImpl>),
            None,
        );

        let _ = result_tx.send(task).await;
        Ok(())
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
        vec!["Gather".to_string()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        // Materialize and gather all partitions to a single node
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            input_node,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        );
        plan_context.spawn(execution_loop);

        SubmittableTaskStream::from(result_rx)
    }
}
