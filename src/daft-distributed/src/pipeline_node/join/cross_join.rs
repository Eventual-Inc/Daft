use std::sync::Arc;

use common_error::DaftResult;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::UnknownClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, stream::select};

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        PipelineNodeImpl, SubmittableTaskStream,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
    utils::channel::{Sender, create_channel},
};

pub(crate) struct CrossJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    left_node: DistributedPipelineNode,
    right_node: DistributedPipelineNode,
}

impl CrossJoinNode {
    const NODE_NAME: NodeName = "CrossJoin";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        num_partitions: usize,
        left_node: DistributedPipelineNode,
        right_node: DistributedPipelineNode,
        output_schema: SchemaRef,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
        );

        let config = PipelineNodeConfig::new(
            output_schema,
            plan_config.config.clone(),
            Arc::new(UnknownClusteringConfig::new(num_partitions).into()),
        );

        Self {
            config,
            context,
            left_node,
            right_node,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    async fn execution_loop(
        self: Arc<Self>,
        left_input: SubmittableTaskStream,
        right_input: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
    ) -> DaftResult<()> {
        let mut left_tasks = Vec::new();
        let mut right_tasks = Vec::new();

        let mut combined_stream = select(
            left_input.map(|task| (true /* is_left */, task)),
            right_input.map(|task| (false /* !is_left */, task)),
        );

        while let Some((is_left, task)) = combined_stream.next().await {
            if is_left {
                left_tasks.push(task);
                for right_task in &right_tasks {
                    self.create_cross_join_task(
                        left_tasks.last().expect("left_tasks should not be empty"),
                        right_task,
                        &task_id_counter,
                        &result_tx,
                    )
                    .await?;
                }
            } else {
                right_tasks.push(task);
                for left_task in &left_tasks {
                    self.create_cross_join_task(
                        left_task,
                        right_tasks.last().expect("right_tasks should not be empty"),
                        &task_id_counter,
                        &result_tx,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    async fn create_cross_join_task(
        &self,
        left_task: &SubmittableTask<SwordfishTask>,
        right_task: &SubmittableTask<SwordfishTask>,
        task_id_counter: &TaskIDCounter,
        result_tx: &Sender<SubmittableTask<SwordfishTask>>,
    ) -> DaftResult<()> {
        let left_plan = left_task.task().plan();
        let right_plan = right_task.task().plan();
        let right_psets = right_task.task().psets().clone();
        let config = right_task.task().config().clone();

        let cross_join_plan = LocalPhysicalPlan::cross_join(
            left_plan,
            right_plan,
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            LocalNodeContext {
                origin_node_id: Some(self.node_id() as usize),
                additional: None,
            },
        );

        let mut psets = right_psets;
        psets.extend(left_task.task().psets().clone());

        let new_task = SwordfishTask::new(
            TaskContext::from((&self.context, task_id_counter.next())),
            cross_join_plan,
            config,
            psets,
            SchedulingStrategy::Spread,
            self.context().to_hashmap(),
        );

        let _ = result_tx.send(SubmittableTask::new(new_task)).await;

        Ok(())
    }
}

impl PipelineNodeImpl for CrossJoinNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.left_node.clone(), self.right_node.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec!["Cross Join".to_string()];
        res.push(format!("Left side: {}", self.left_node.name()));
        res.push(format!("Right side: {}", self.right_node.name()));
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let left_input = self.left_node.clone().produce_tasks(plan_context);
        let right_input = self.right_node.clone().produce_tasks(plan_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            left_input,
            right_input,
            plan_context.task_id_counter(),
            result_tx,
        );
        plan_context.spawn(execution_loop);

        SubmittableTaskStream::from(result_rx)
    }
}
