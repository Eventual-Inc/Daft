use std::sync::Arc;

use common_display::{DisplayLevel, tree::TreeDisplay};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{partitioning::UnknownClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        SubmittableTaskStream,
    },
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{Sender, create_channel},
};

pub(crate) struct CrossJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    left_node: Arc<dyn DistributedPipelineNode>,
    right_node: Arc<dyn DistributedPipelineNode>,
}

impl CrossJoinNode {
    const NODE_NAME: NodeName = "CrossJoin";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        num_partitions: usize,
        left_node: Arc<dyn DistributedPipelineNode>,
        right_node: Arc<dyn DistributedPipelineNode>,
        output_schema: SchemaRef,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![left_node.node_id(), right_node.node_id()],
            vec![left_node.name(), right_node.name()],
            logical_node_id,
        );

        let config = PipelineNodeConfig::new(
            output_schema,
            stage_config.config.clone(),
            Arc::new(UnknownClusteringConfig::new(num_partitions).into()),
        );

        Self {
            config,
            context,
            left_node,
            right_node,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec!["Cross Join".to_string()];
        res.push(format!("Left side: {}", self.left_node.name()));
        res.push(format!("Right side: {}", self.right_node.name()));
        res
    }

    async fn execution_loop(
        self: Arc<Self>,
        mut left_input: SubmittableTaskStream,
        mut right_input: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
    ) -> DaftResult<()> {
        let mut left_tasks = Vec::new();
        while let Some(left_task) = left_input.next().await {
            left_tasks.push(left_task);
        }

        while let Some(right_task) = right_input.next().await {
            let right_plan = right_task.task().plan().clone();
            let right_psets = right_task.task().psets().clone();
            let config = right_task.task().config().clone();

            for left_task in &left_tasks {
                let left_plan = left_task.task().plan();
                let cross_join_plan = LocalPhysicalPlan::cross_join(
                    left_plan,
                    right_plan.clone(),
                    self.config.schema.clone(),
                    StatsState::NotMaterialized,
                );

                let mut psets = right_psets.clone();
                psets.extend(left_task.task().psets().clone());

                let new_task = SwordfishTask::new(
                    TaskContext::from((self.context(), task_id_counter.next())),
                    cross_join_plan,
                    config.clone(),
                    psets,
                    SchedulingStrategy::Spread,
                    self.context().to_hashmap(),
                );

                let task = SubmittableTask::new(new_task);

                if result_tx.send(task).await.is_err() {
                    break;
                }
            }
        }
        Ok(())
    }
}

impl TreeDisplay for CrossJoinNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        match level {
            DisplayLevel::Compact => self.get_name(),
            _ => self.multiline_display().join("\n"),
        }
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![
            self.left_node.as_tree_display(),
            self.right_node.as_tree_display(),
        ]
    }

    fn get_name(&self) -> String {
        Self::NODE_NAME.to_string()
    }
}

impl DistributedPipelineNode for CrossJoinNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.left_node.clone(), self.right_node.clone()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let left_input = self.left_node.clone().produce_tasks(stage_context);
        let right_input = self.right_node.clone().produce_tasks(stage_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            left_input,
            right_input,
            stage_context.task_id_counter(),
            result_tx,
        );
        stage_context.spawn(execution_loop);

        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
