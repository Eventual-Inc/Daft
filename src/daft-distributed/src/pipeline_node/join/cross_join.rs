use std::sync::Arc;

use common_error::DaftResult;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::UnknownClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, stream::select};

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        PipelineNodeImpl, TaskBuilderStream,
    },
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
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
        left_input: TaskBuilderStream,
        right_input: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        let mut left_builders: Vec<SwordfishTaskBuilder> = Vec::new();
        let mut right_builders: Vec<SwordfishTaskBuilder> = Vec::new();

        let mut combined_stream = select(
            left_input.map(|builder| (true /* is_left */, builder)),
            right_input.map(|builder| (false /* !is_left */, builder)),
        );

        while let Some((is_left, builder)) = combined_stream.next().await {
            if is_left {
                // Create cross joins with all existing right builders
                for right_builder in &right_builders {
                    let new_builder = SwordfishTaskBuilder::combine_with(
                        &builder,
                        right_builder,
                        self.as_ref(),
                        |left_plan, right_plan| {
                            LocalPhysicalPlan::cross_join(
                                left_plan,
                                right_plan,
                                self.config.schema.clone(),
                                StatsState::NotMaterialized,
                                LocalNodeContext {
                                    origin_node_id: Some(self.node_id() as usize),
                                    additional: None,
                                },
                            )
                        },
                    );
                    let _ = result_tx.send(new_builder).await;
                }
                left_builders.push(builder);
            } else {
                // Create cross joins with all existing left builders
                for left_builder in &left_builders {
                    let new_builder = SwordfishTaskBuilder::combine_with(
                        left_builder,
                        &builder,
                        self.as_ref(),
                        |left_plan, right_plan| {
                            LocalPhysicalPlan::cross_join(
                                left_plan,
                                right_plan,
                                self.config.schema.clone(),
                                StatsState::NotMaterialized,
                                LocalNodeContext {
                                    origin_node_id: Some(self.node_id() as usize),
                                    additional: None,
                                },
                            )
                        },
                    );
                    let _ = result_tx.send(new_builder).await;
                }
                right_builders.push(builder);
            }
        }

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
    ) -> TaskBuilderStream {
        let left_input = self.left_node.clone().produce_tasks(plan_context);
        let right_input = self.right_node.clone().produce_tasks(plan_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(left_input, right_input, result_tx);
        plan_context.spawn(execution_loop);

        TaskBuilderStream::from(result_rx)
    }
}
