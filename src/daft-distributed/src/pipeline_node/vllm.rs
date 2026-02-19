use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_dsl::expr::bound_expr::BoundVLLMExpr;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        PipelineNodeImpl, TaskBuilderStream,
    },
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::{Sender, create_channel},
};

pub(crate) struct VLLMNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    expr: BoundVLLMExpr,
    output_column_name: Arc<str>,
    child: DistributedPipelineNode,
}

impl VLLMNode {
    const NODE_NAME: NodeName = "VLLM";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        expr: BoundVLLMExpr,
        output_column_name: Arc<str>,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
            NodeType::VLLMProject,
            NodeCategory::Intermediate,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            expr,
            output_column_name,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    #[cfg(feature = "python")]
    async fn execution_loop(
        self: Arc<Self>,
        mut input_task_stream: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        use common_runtime::JoinSet;
        use daft_dsl::functions::python::RuntimePyObject;
        use pyo3::{PyErr, Python, intern, types::PyAnyMethods};

        let expr = self.expr.inner();

        let llm_actors = RuntimePyObject::new(Arc::new(Python::attach(|py| {
            Ok::<_, PyErr>(
                py.import(intern!(py, "daft.execution.vllm"))?
                    .getattr(intern!(py, "LLMActors"))?
                    .call1((
                        &expr.model,
                        expr.engine_args.as_ref(),
                        expr.generate_args.as_ref(),
                        expr.gpus_per_actor,
                        expr.concurrency,
                        expr.load_balance_threshold,
                    ))?
                    .unbind(),
            )
        })?));

        let mut running_tasks = JoinSet::new();
        while let Some(builder) = input_task_stream.next().await {
            let modified_builder = builder.map_plan(self.as_ref(), |input| {
                use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
                use daft_logical_plan::stats::StatsState;

                LocalPhysicalPlan::vllm_project(
                    input,
                    self.expr.clone(),
                    Some(llm_actors.clone()),
                    self.output_column_name.clone(),
                    self.config.schema.clone(),
                    StatsState::NotMaterialized,
                    LocalNodeContext {
                        origin_node_id: Some(self.node_id() as usize),
                        additional: None,
                    },
                )
            });

            let (builder_with_token, notify_token) = modified_builder.add_notify_token();
            running_tasks.spawn(notify_token);
            if result_tx.send(builder_with_token).await.is_err() {
                break;
            }
        }
        // Wait for all tasks to finish.
        while let Some(result) = running_tasks.join_next().await {
            if result?.is_err() {
                break;
            }
        }

        Ok(())
    }

    #[cfg(not(feature = "python"))]
    async fn execution_loop(
        self: Arc<Self>,
        mut input_task_stream: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        unimplemented!("VLLM is not supported in non-Python mode");
    }
}

impl PipelineNodeImpl for VLLMNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> super::TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(input_node, result_tx);
        plan_context.spawn(execution_loop);

        TaskBuilderStream::from(result_rx)
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        vec![format!("VLLM: {}", self.expr)]
    }
}
