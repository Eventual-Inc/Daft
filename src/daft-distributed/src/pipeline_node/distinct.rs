use std::sync::Arc;

use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::HashClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, PipelineNodeImpl, SubmittableTaskStream};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    plan::{PlanConfig, PlanExecutionContext},
};

pub(crate) struct DistinctNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    columns: Vec<BoundExpr>,
    child: DistributedPipelineNode,
}

impl DistinctNode {
    const NODE_NAME: NodeName = "Distinct";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        columns: Vec<BoundExpr>,
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

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for DistinctNode {
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
        let mut res = vec![];
        res.push(format!(
            "Distinct: By {}",
            self.columns.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        // Pipeline the distinct op
        let self_clone = self.clone();
        input_node.pipeline_instruction(self, move |input| {
            LocalPhysicalPlan::dedup(
                input,
                self_clone.columns.clone(),
                self_clone.config.schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(self_clone.node_id() as usize),
                    additional: None,
                },
            )
        })
    }
}
