use std::sync::Arc;

use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, PipelineNodeImpl, SubmittableTaskStream};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    plan::{PlanConfig, PlanExecutionContext},
};

pub(crate) struct FilterNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    predicate: BoundExpr,
    child: DistributedPipelineNode,
}

impl FilterNode {
    const NODE_NAME: NodeName = "Filter";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        predicate: BoundExpr,
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
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            predicate,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for FilterNode {
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
        vec![format!("Filter: {}", self.predicate)]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let predicate = self.predicate.clone();
        let node_id = self.node_id();
        input_node.pipeline_instruction(self, move |input| {
            LocalPhysicalPlan::filter(
                input,
                predicate.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            )
        })
    }
}
