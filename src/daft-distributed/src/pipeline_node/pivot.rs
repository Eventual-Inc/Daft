use std::sync::Arc;

use daft_dsl::expr::bound_expr::{BoundAggExpr, BoundExpr};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use super::{PipelineNodeImpl, SubmittableTaskStream};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
    },
    plan::{PlanConfig, PlanExecutionContext},
};

pub(crate) struct PivotNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    group_by: Vec<BoundExpr>,
    pivot_column: BoundExpr,
    value_column: BoundExpr,
    aggregation: BoundAggExpr,
    names: Vec<String>,
    child: DistributedPipelineNode,
}

impl PivotNode {
    const NODE_NAME: NodeName = "Pivot";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        group_by: Vec<BoundExpr>,
        pivot_column: BoundExpr,
        value_column: BoundExpr,
        aggregation: BoundAggExpr,
        names: Vec<String>,
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
            group_by,
            pivot_column,
            value_column,
            aggregation,
            names,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for PivotNode {
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
        use itertools::Itertools;
        vec![
            "Pivot:".to_string(),
            format!(
                "Group By = {}",
                self.group_by.iter().map(|e| e.to_string()).join(", ")
            ),
            format!("Pivot Column = {}", self.pivot_column),
            format!("Value Column = {}", self.value_column),
            format!("Aggregation = {}", self.aggregation),
            format!("Pivoted Columns = {}", self.names.iter().join(", ")),
            format!("Output Schema = {}", self.config.schema.short_string()),
        ]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

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
                LocalNodeContext {
                    origin_node_id: Some(self_clone.node_id() as usize),
                    additional: None,
                },
            )
        };

        input_node.pipeline_instruction(self, plan_builder)
    }
}
