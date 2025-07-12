use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{partitioning::HashClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    stage::{StageConfig, StageExecutionContext},
};

pub(crate) struct DistinctNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    columns: Vec<BoundExpr>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl DistinctNode {
    const NODE_NAME: NodeName = "Distinct";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        columns: Vec<BoundExpr>,
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
            Arc::new(
                HashClusteringConfig::new(
                    child.config().clustering_spec.num_partitions(),
                    columns.clone().into_iter().map(|e| e.into()).collect(),
                )
                .into(),
            ),
        );
        Self {
            config,
            context,
            columns,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec![];
        res.push(format!(
            "Distinct: By {}",
            self.columns.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }
}

impl TreeDisplay for DistinctNode {
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

impl DistributedPipelineNode for DistinctNode {
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

        // Pipeline the distinct op
        let self_clone = self.clone();
        input_node.pipeline_instruction(stage_context, self.clone(), move |input| {
            Ok(LocalPhysicalPlan::dedup(
                input,
                self_clone.columns.clone(),
                self_clone.config.schema.clone(),
                StatsState::NotMaterialized,
            ))
        })
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
