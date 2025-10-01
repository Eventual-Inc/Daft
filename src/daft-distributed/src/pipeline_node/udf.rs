use std::sync::Arc;

use daft_dsl::{expr::bound_expr::BoundExpr, functions::python::UDFProperties};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{partitioning::translate_clustering_spec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use itertools::Itertools;

use super::DistributedPipelineNode;
use crate::{
    pipeline_node::{
        DistributedPipelineNodeWrapper, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        SubmittableTaskStream,
    },
    plan::{PlanConfig, PlanExecutionContext},
};

pub(crate) struct UDFNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    project: BoundExpr,
    passthrough_columns: Vec<BoundExpr>,
    udf_properties: UDFProperties,
    child: DistributedPipelineNodeWrapper,
}

impl UDFNode {
    const NODE_NAME: NodeName = "UDF";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        plan_config: &PlanConfig,
        project: BoundExpr,
        passthrough_columns: Vec<BoundExpr>,
        udf_properties: UDFProperties,
        schema: SchemaRef,
        child: DistributedPipelineNodeWrapper,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.plan_id,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id()],
            vec![child.name()],
            logical_node_id,
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
            project,
            passthrough_columns,
            udf_properties,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNodeWrapper {
        DistributedPipelineNodeWrapper::new(Arc::new(self))
    }
}

impl DistributedPipelineNode for UDFNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNodeWrapper> {
        vec![self.child.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec![];
        res.push("UDF Executor:".to_string());
        res.push(format!(
            "UDF {} = {}",
            self.udf_properties.name, self.project
        ));
        res.push(format!(
            "Passthrough Columns = [{}]",
            self.passthrough_columns.iter().join(", ")
        ));
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

        let project = self.project.clone();
        let passthrough_columns = self.passthrough_columns.clone();
        let schema = self.config.schema.clone();
        let plan_builder = move |input: LocalPhysicalPlanRef| -> LocalPhysicalPlanRef {
            LocalPhysicalPlan::udf_project(
                input,
                project.clone(),
                passthrough_columns.clone(),
                schema.clone(),
                StatsState::NotMaterialized,
            )
        };

        input_node.pipeline_instruction(self, plan_builder)
    }
}
