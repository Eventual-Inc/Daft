use std::sync::Arc;

use daft_logical_plan::{
    ClusteringSpec,
    partitioning::{ClusteringSpecRef, UnknownClusteringConfig},
};
use daft_schema::prelude::SchemaRef;
use futures::StreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        PipelineNodeImpl, SubmittableTaskStream,
    },
    plan::{PlanConfig, PlanExecutionContext},
};

pub(crate) struct ConcatNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    child: DistributedPipelineNode,
    other: DistributedPipelineNode,
}

impl ConcatNode {
    const NODE_NAME: NodeName = "Concat";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        schema: SchemaRef,
        other: DistributedPipelineNode,
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
            ClusteringSpecRef::new(ClusteringSpec::Unknown(UnknownClusteringConfig::new(
                child.config().clustering_spec.num_partitions()
                    + other.config().clustering_spec.num_partitions(),
            ))),
        );

        Self {
            config,
            context,
            child,
            other,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for ConcatNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone(), self.other.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        vec!["Concat".to_string()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let other_node = self.other.clone().produce_tasks(plan_context);
        SubmittableTaskStream::new(input_node.chain(other_node).boxed())
    }
}
