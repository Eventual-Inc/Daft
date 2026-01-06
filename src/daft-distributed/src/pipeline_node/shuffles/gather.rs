use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::UnknownClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        PipelineNodeImpl, TaskBuilderStream,
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
        input_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Trigger materialization of all inputs
        let materialized = input_node
            .materialize(
                scheduler_handle.clone(),
                self.context.query_idx,
                task_id_counter.clone(),
            )
            .try_collect::<Vec<_>>()
            .await?;

        let in_memory_source_plan = LocalPhysicalPlan::in_memory_scan(
            self.node_id().to_string(),
            self.config.schema.clone(),
            materialized
                .iter()
                .map(|output| output.size_bytes())
                .sum::<usize>(),
            StatsState::NotMaterialized,
            LocalNodeContext {
                origin_node_id: Some(self.node_id() as usize),
                additional: None,
            },
        );
        let partition_refs = materialized
            .into_iter()
            .flat_map(|output| output.into_inner().0)
            .collect::<Vec<_>>();
        let plan = in_memory_source_plan;
        let psets = HashMap::from([(self.node_id().to_string(), partition_refs)]);
        let builder = SwordfishTaskBuilder::new(plan, self.as_ref()).with_psets(psets);

        let _ = result_tx.send(builder).await;
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
    ) -> TaskBuilderStream {
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

        TaskBuilderStream::from(result_rx)
    }
}
