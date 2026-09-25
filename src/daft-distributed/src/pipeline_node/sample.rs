use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef, SamplingMethod};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use super::{PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{
        ClusteringStrategy, DistributedPipelineNode, MaterializedOutput, NodeID,
        PipelineNodeConfig, PipelineNodeContext, clustering::BoundClusteringSpec,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::channel::{Sender, create_channel},
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
    const NODE_NAME: &'static str = "Sample";
    const SIZE_SAMPLE_PHASE: &'static str = "size-sample";

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
        let sampling_method = if let Some(fraction) = fraction {
            SamplingMethod::Fraction(fraction)
        } else if let Some(size) = size {
            SamplingMethod::Size(size)
        } else {
            panic!("Either fraction or size must be specified for sample");
        };
        let node_category = match sampling_method {
            SamplingMethod::Fraction(_) => NodeCategory::Intermediate,
            SamplingMethod::Size(_) => NodeCategory::BlockingSink,
        };
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::Sample,
            node_category,
        );
        let clustering_strategy = match sampling_method {
            SamplingMethod::Fraction(_) => ClusteringStrategy::Passthrough { child: &child },
            SamplingMethod::Size(_) => {
                ClusteringStrategy::Explicit(BoundClusteringSpec::unknown(1))
            }
        };
        let config =
            PipelineNodeConfig::new(schema, plan_config.config.clone(), clustering_strategy);
        Self {
            config,
            context,
            sampling_method,
            with_replacement,
            seed,
            child,
        }
    }

    async fn size_execution_loop(
        self: Arc<Self>,
        input_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let materialized_outputs = input_node
            .materialize(scheduler_handle, self.context.query_idx, task_id_counter)
            .try_collect::<Vec<MaterializedOutput>>()
            .await?;

        let (in_memory_scan, psets) = MaterializedOutput::into_in_memory_scan_with_psets_and_phase(
            materialized_outputs,
            self.config.schema.clone(),
            self.node_id(),
            Self::SIZE_SAMPLE_PHASE,
        );
        let plan = LocalPhysicalPlan::sample(
            in_memory_scan,
            self.sampling_method,
            self.with_replacement,
            self.seed,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(self.node_id() as usize))
                .with_phase(Self::SIZE_SAMPLE_PHASE),
        );
        let task = SwordfishTaskBuilder::new(plan, self.as_ref(), self.node_id())
            .with_psets(self.node_id(), psets);
        let _ = result_tx.send(task).await;
        Ok(())
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
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        match self.sampling_method {
            SamplingMethod::Fraction(_) => {
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
                        LocalNodeContext::new(Some(node_id as usize)),
                    )
                };

                input_node.pipeline_instruction(self, plan_builder)
            }
            SamplingMethod::Size(_) => {
                let (result_tx, result_rx) = create_channel(1);
                plan_context.spawn(self.size_execution_loop(
                    input_node,
                    plan_context.task_id_counter(),
                    result_tx,
                    plan_context.scheduler_handle(),
                ));
                TaskBuilderStream::from(result_rx)
            }
        }
    }
}
