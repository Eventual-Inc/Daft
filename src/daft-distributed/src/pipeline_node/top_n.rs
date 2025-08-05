use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, SubmittableTaskStream};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    stage::{StageConfig, StageExecutionContext},
};

pub(crate) struct TopNNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    // Sort properties
    sort_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    limit: u64,
    child: Arc<dyn DistributedPipelineNode>,
}

impl TopNNode {
    const NODE_NAME: NodeName = "TopN";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        limit: u64,
        output_schema: SchemaRef,
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
            output_schema,
            stage_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            sort_by,
            descending,
            nulls_first,
            limit,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec!["TopN".to_string()];
        res.push(format!(
            "Sort by: {}",
            self.sort_by.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Descending = {}",
            self.descending.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Nulls first = {}",
            self.nulls_first.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Limit = {}", self.limit));
        res
    }
}

impl TreeDisplay for TopNNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.context.node_name).unwrap();
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
        self.context.node_name.to_string()
    }
}

impl DistributedPipelineNode for TopNNode {
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

        // Pipeline the top-n
        let self_clone = self.clone();
        input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::top_n(
                input,
                self_clone.sort_by.clone(),
                self_clone.descending.clone(),
                self_clone.nulls_first.clone(),
                self_clone.limit,
                None, // TODO(zhenchao) support offset
                StatsState::NotMaterialized,
            )
        })
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
