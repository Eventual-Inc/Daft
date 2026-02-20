use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
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
    num_partitions: usize,
    child: DistributedPipelineNode,
}

impl RepartitionNode {
    const NODE_NAME: NodeName = "Repartition";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
            NodeType::Repartition,
            NodeCategory::BlockingSink,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            repartition_spec
                .to_clustering_spec(child.config().clustering_spec.num_partitions())
                .into(),
        );

        Self {
            config,
            context,
            repartition_spec,
            num_partitions,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    // Async execution to get all partitions out
    async fn execution_loop(
        self: Arc<Self>,
        local_repartition_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Trigger materialization of the partitions
        // This will produce a stream of materialized outputs, each containing a vector of num_partitions partitions
        let materialized_stream = local_repartition_node.materialize(
            scheduler_handle,
            self.context.query_idx,
            task_id_counter,
        );
        let transposed_outputs =
            transpose_materialized_outputs_from_stream(materialized_stream, self.num_partitions)
                .await?;

        // Make each partition group (partitions equal by (hash % num_partitions)) input to a in-memory scan
        for partition_group in transposed_outputs {
            let (in_memory_source_plan, psets) = MaterializedOutput::into_in_memory_scan_with_psets(
                partition_group,
                self.config.schema.clone(),
                self.node_id(),
            );
            let builder = SwordfishTaskBuilder::new(in_memory_source_plan, self.as_ref())
                .with_psets(self.node_id(), psets);

            let _ = result_tx.send(builder).await;
        }

        Ok(())
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

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec![format!("Repartition: {}", self.repartition_spec.var_name())];
        res.extend(self.repartition_spec.multiline_display());
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        // First pipeline the local repartition op
        let self_clone = self.clone();
        let local_repartition_node = input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::repartition(
                input,
                self_clone.repartition_spec.clone(),
                self_clone.num_partitions,
                self_clone.config.schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(self_clone.node_id() as usize),
                    additional: None,
                },
            )
        });

        let (result_tx, result_rx) = create_channel(1);

        plan_context.spawn(self.execution_loop(
            local_repartition_node,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        ));
        TaskBuilderStream::from(result_rx)
    }
}
