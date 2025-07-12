use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_logical_plan::partitioning::UnknownClusteringConfig;
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::{
        make_in_memory_scan_from_materialized_outputs, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, PipelineOutput,
    },
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct GatherNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    child: Arc<dyn DistributedPipelineNode>,
}

impl GatherNode {
    const NODE_NAME: NodeName = "Gather";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id()],
            vec![child.name()],
            logical_node_id,
        );
        let config = PipelineNodeConfig::new(
            schema,
            stage_config.config.clone(),
            Arc::new(UnknownClusteringConfig::new(1).into()),
        );
        Self {
            config,
            context,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["Gather".to_string()]
    }

    // Async execution to get all partitions out
    async fn execution_loop(
        self: Arc<Self>,
        input_node: RunningPipelineNode,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Trigger materialization of all inputs
        let materialized = input_node
            .materialize(scheduler_handle.clone())
            .try_collect::<Vec<_>>()
            .await?;

        let self_clone = self.clone();
        let task = make_in_memory_scan_from_materialized_outputs(
            TaskContext::from((&self_clone.context, task_id_counter.next())),
            materialized,
            &(self_clone as Arc<dyn DistributedPipelineNode>),
        )?;

        let _ = result_tx.send(PipelineOutput::Task(task)).await;
        Ok(())
    }
}

impl TreeDisplay for GatherNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.context.node_name).unwrap();
            }
            _ => {
                let multiline_display = self.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.context.node_name.to_string()
    }
}

impl DistributedPipelineNode for GatherNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageExecutionContext) -> RunningPipelineNode {
        let input_node = self.child.clone().start(stage_context);

        // Materialize and gather all partitions to a single node
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            input_node,
            stage_context.task_id_counter(),
            result_tx,
            stage_context.scheduler_handle(),
        );
        stage_context.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
