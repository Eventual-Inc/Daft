use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{partitioning::translate_clustering_spec, stats::StatsState};
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    stage::{StageConfig, StageExecutionContext},
};

pub(crate) struct ProjectNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    projection: Vec<BoundExpr>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl ProjectNode {
    const NODE_NAME: NodeName = "Project";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        projection: Vec<BoundExpr>,
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

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
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
}

impl TreeDisplay for ProjectNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.name()).unwrap();
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
        self.name().to_string()
    }
}

impl DistributedPipelineNode for ProjectNode {
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

        let projection = self.projection.clone();
        let schema = self.config.schema.clone();
        let plan_builder = move |input: LocalPhysicalPlanRef| -> DaftResult<LocalPhysicalPlanRef> {
            Ok(LocalPhysicalPlan::project(
                input,
                projection.clone(),
                schema.clone(),
                StatsState::NotMaterialized,
            ))
        };

        input_node.pipeline_instruction(stage_context, self, plan_builder)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
