use std::sync::Arc;

use daft_dsl::{expr::bound_expr::BoundExpr, functions::python::UDFProperties};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{partitioning::translate_clustering_spec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use itertools::Itertools;

use super::PipelineNodeImpl;
use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        SubmittableTaskStream,
    },
    plan::{PlanConfig, PlanExecutionContext},
};

pub(crate) struct UDFNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    expr: BoundExpr,
    udf_properties: UDFProperties,
    passthrough_columns: Vec<BoundExpr>,
    child: DistributedPipelineNode,
}

impl UDFNode {
    const NODE_NAME: NodeName = "UDF";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        expr: BoundExpr,
        udf_properties: UDFProperties,
        passthrough_columns: Vec<BoundExpr>,
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
                &passthrough_columns
                    .iter()
                    .map(|e| e.inner().clone())
                    .collect::<Vec<_>>(),
            ),
        );
        Self {
            config,
            context,
            expr,
            udf_properties,
            passthrough_columns,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for UDFNode {
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
        let mut res = vec![
            format!("UDF: {}", self.udf_properties.name),
            format!("Expr = {}", self.expr),
            format!(
                "Passthrough Columns = [{}]",
                self.passthrough_columns.iter().join(", ")
            ),
            format!(
                "Properties = {{ {} }}",
                self.udf_properties.multiline_display(false).join(", ")
            ),
        ];

        if let Some(resource_request) = &self.udf_properties.resource_request {
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

        let expr = self.expr.clone();
        let udf_properties = self.udf_properties.clone();
        let passthrough_columns = self.passthrough_columns.clone();
        let schema = self.config.schema.clone();
        let node_id = self.context.node_id;
        let plan_builder = move |input: LocalPhysicalPlanRef| -> LocalPhysicalPlanRef {
            LocalPhysicalPlan::udf_project(
                input,
                expr.clone(),
                udf_properties.clone(),
                passthrough_columns.clone(),
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
