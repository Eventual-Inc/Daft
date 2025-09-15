use std::sync::Arc;

use common_display::{DisplayLevel, tree::TreeDisplay};
use daft_dsl::expr::bound_expr::{BoundAggExpr, BoundExpr};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, SubmittableTaskStream};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    stage::{StageConfig, StageExecutionContext},
};

pub(crate) struct PivotNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    group_by: Vec<BoundExpr>,
    pivot_column: BoundExpr,
    value_column: BoundExpr,
    aggregation: BoundAggExpr,
    names: Vec<String>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl PivotNode {
    const NODE_NAME: NodeName = "Pivot";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        group_by: Vec<BoundExpr>,
        pivot_column: BoundExpr,
        value_column: BoundExpr,
        aggregation: BoundAggExpr,
        names: Vec<String>,
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
            group_by,
            pivot_column,
            value_column,
            aggregation,
            names,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec![];
        res.push(format!("Pivot: {}", self.aggregation));
        res.push(format!(
            "Group by = {}",
            self.group_by.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Pivot column: {}", self.pivot_column));
        res.push(format!("Value column: {}", self.value_column));
        res.push(format!("Pivoted columns: {}", self.names.iter().join(", ")));
        res
    }
}

impl TreeDisplay for PivotNode {
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

impl DistributedPipelineNode for PivotNode {
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

        let self_clone = self.clone();
        let plan_builder = move |input: LocalPhysicalPlanRef| -> LocalPhysicalPlanRef {
            LocalPhysicalPlan::pivot(
                input,
                self_clone.group_by.clone(),
                self_clone.pivot_column.clone(),
                self_clone.value_column.clone(),
                self_clone.aggregation.clone(),
                self_clone.names.clone(),
                false,
                self_clone.config.schema.clone(),
                StatsState::NotMaterialized,
            )
        };

        input_node.pipeline_instruction(self.clone(), plan_builder)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
