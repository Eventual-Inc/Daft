use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use daft_dsl::expr::{bound_expr::BoundExpr, BoundColumn};
use daft_functions_uri::UrlDownloadArgs;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, SubmittableTaskStream};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    stage::{StageConfig, StageExecutionContext},
};

pub(crate) struct UrlDownloadNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    args: UrlDownloadArgs<BoundExpr>,
    output_column: String,
    passthrough_columns: Vec<BoundColumn>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl UrlDownloadNode {
    const NODE_NAME: NodeName = "UrlDownload";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        args: UrlDownloadArgs<BoundExpr>,
        output_column: String,
        passthrough_columns: Vec<BoundColumn>,
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
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            args,
            output_column,
            passthrough_columns,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["URL Download".to_string()]
    }
}

impl TreeDisplay for UrlDownloadNode {
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

impl DistributedPipelineNode for UrlDownloadNode {
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

        let args = self.args.clone();
        let output_column = self.output_column.clone();
        let schema = self.config.schema.clone();
        let passthrough_columns = self.passthrough_columns.clone();
        let plan_builder = move |input: LocalPhysicalPlanRef| -> LocalPhysicalPlanRef {
            LocalPhysicalPlan::url_download(
                input,
                args.clone(),
                passthrough_columns.clone(),
                output_column.clone(),
                schema.clone(),
                StatsState::NotMaterialized,
            )
        };

        input_node.pipeline_instruction(self.clone(), plan_builder)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
