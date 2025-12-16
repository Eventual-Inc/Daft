use std::sync::Arc;

use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef, SamplingMethod};
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
    sampling_method: SamplingMethod,
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
        fraction: Option<f64>,
        size: Option<usize>,
        with_replacement: bool,
        seed: Option<u64>,
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
        let sampling_method = if let Some(fraction) = fraction {
            SamplingMethod::Fraction(fraction)
        } else if let Some(size) = size {
            SamplingMethod::Size(size)
        } else {
            panic!("Either fraction or size must be specified for sample");
        };
        Self {
            config,
            context,
            sampling_method,
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
        match &self.sampling_method {
            SamplingMethod::Fraction(fraction) => {
                res.push(format!("Sample: {} (fraction)", fraction));
            }
            SamplingMethod::Size(size) => res.push(format!("Sample: {} rows", size)),
        }
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
        let sampling_method = self.sampling_method;
        let with_replacement = self.with_replacement;
        let seed = self.seed;
        let node_id = self.node_id();
        let plan_builder = move |input: LocalPhysicalPlanRef| -> LocalPhysicalPlanRef {
            LocalPhysicalPlan::sample(
                input,
                sampling_method,
                with_replacement,
                seed,
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            )
        };

        input_node.pipeline_instruction(self, plan_builder)
    }
}
