use std::sync::Arc;

use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{partitioning::translate_clustering_spec, stats::StatsState};
use daft_schema::schema::SchemaRef;

use super::{PipelineNodeImpl, SubmittableTaskStream};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
    },
    plan::{PlanConfig, PlanExecutionContext},
};

pub(crate) struct ProjectNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    projection: Vec<BoundExpr>,
    child: DistributedPipelineNode,
}

impl ProjectNode {
    const NODE_NAME: NodeName = "Project";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        projection: Vec<BoundExpr>,
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
            translate_clustering_spec(
                child.config().clustering_spec.clone(),
                &projection
                    .iter()
                    .map(|e| e.inner().clone())
                    .collect::<Vec<_>>(),
            ),
        );
        Self {
            config,
            context,
            projection,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for ProjectNode {
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
        use daft_dsl::functions::python::get_resource_request;
        use itertools::Itertools;
        let mut res = vec![];
        res.push(format!(
            "Project: {}",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        ));
        if let Some(resource_request) = get_resource_request(&self.projection) {
            let multiline_display = resource_request.multiline_display();
            res.push(format!(
                "Resource request = {{ {} }}",
                multiline_display.join(", ")
            ));
        } else {
            res.push("Resource request = None".to_string());
        }
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let projection = self.projection.clone();
        let schema = self.config.schema.clone();
        let node_id = self.node_id();
        let plan_builder = move |input: LocalPhysicalPlanRef| -> LocalPhysicalPlanRef {
            LocalPhysicalPlan::project(
                input,
                projection.clone(),
                schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            )
        };

        input_node.pipeline_instruction(self, plan_builder)
    }
}
