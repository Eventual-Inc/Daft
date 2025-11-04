use std::sync::Arc;

use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef, SamplingMethod};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use super::{PipelineNodeImpl, SubmittableTaskStream};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
    },
    plan::{PlanConfig, PlanExecutionContext},
};

pub(crate) struct SampleNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    fraction: f64,
    with_replacement: bool,
    seed: Option<u64>,
    child: DistributedPipelineNode,
}

impl SampleNode {
    const NODE_NAME: NodeName = "Sample";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.plan_id,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id()],
            vec![child.name()],
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            fraction,
            with_replacement,
            seed,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for SampleNode {
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
        let mut res = vec![];
        res.push(format!("Sample: {}", self.fraction));
        res.push(format!("With replacement = {}", self.with_replacement));
        res.push(format!("Seed = {:?}", self.seed));
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        // Create the plan builder closure
        let fraction = self.fraction;
        let with_replacement = self.with_replacement;
        let seed = self.seed;
        let plan_builder = move |input: LocalPhysicalPlanRef| -> LocalPhysicalPlanRef {
            LocalPhysicalPlan::sample(
                input,
                SamplingMethod::Fraction(fraction),
                with_replacement,
                seed,
                StatsState::NotMaterialized,
            )
        };

        input_node.pipeline_instruction(self, plan_builder)
    }
}
