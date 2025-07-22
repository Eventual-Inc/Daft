use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use daft_dsl::{
    expr::bound_expr::BoundExpr,
    functions::python::{get_resource_request, get_udf_names},
};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{partitioning::translate_clustering_spec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use itertools::Itertools;

use super::DistributedPipelineNode;
use crate::{
    pipeline_node::{
        NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext, SubmittableTaskStream,
    },
    stage::{StageConfig, StageExecutionContext},
};

pub(crate) struct UDFNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    project: BoundExpr,
    passthrough_columns: Vec<BoundExpr>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl UDFNode {
    const NODE_NAME: NodeName = "UDF";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        project: BoundExpr,
        passthrough_columns: Vec<BoundExpr>,
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
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("UDF Executor:".to_string());
        res.push(format!(
            "UDF {} = {}",
            get_udf_names(self.project.inner()).first().unwrap(),
            self.project
        ));
        res.push(format!(
            "Passthrough Columns = [{}]",
            self.passthrough_columns.iter().join(", ")
        ));
        if let Some(resource_request) = get_resource_request(&[self.project.clone()]) {
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
}

impl TreeDisplay for UDFNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        match level {
            DisplayLevel::Compact => self.name().to_string(),
            _ => self.multiline_display().join("\n"),
        }
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.name().to_string()
    }
}

impl DistributedPipelineNode for UDFNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(stage_context);

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

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
