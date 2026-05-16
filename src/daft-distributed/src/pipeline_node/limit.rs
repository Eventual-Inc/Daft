use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
#[cfg(feature = "python")]
use common_py_serde::PyObjectWrapper;
use common_runtime::JoinSet;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;
#[cfg(feature = "python")]
use pyo3::{Py, PyAny, Python, types::PyAnyMethods};

use super::{DistributedPipelineNode, PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{NodeID, PipelineNodeConfig, PipelineNodeContext},
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::{Sender, create_channel},
};

/// Lazily-initialized handle to the Ray `LimitCounterActor`.
#[cfg(feature = "python")]
#[derive(Debug)]
enum LimitActor {
    Uninitialized { limit: u64, offset: u64 },
    Initialized(PyObjectWrapper),
}

#[cfg(feature = "python")]
impl LimitActor {
    async fn get_or_start(&mut self, timeout: usize) -> DaftResult<PyObjectWrapper> {
        match self {
            Self::Uninitialized { limit, offset } => {
                let (limit, offset) = (*limit, *offset);
                let actor: Py<PyAny> =
                    common_runtime::python::execute_python_coroutine::<_, Py<PyAny>>(move |py| {
                        let module =
                            py.import(pyo3::intern!(py, "daft.execution.ray_distributed_limit"))?;
                        let coroutine = module.call_method1(
                            pyo3::intern!(py, "start_limit_counter_actor"),
                            (limit as i64, offset as i64, timeout as i64),
                        )?;
                        Ok(coroutine)
                    })
                    .await?;
                let wrapper = PyObjectWrapper(Arc::new(actor));
                *self = Self::Initialized(wrapper.clone());
                Ok(wrapper)
            }
            Self::Initialized(wrapper) => Ok(wrapper.clone()),
        }
    }

    fn teardown(&mut self) {
        if let Self::Initialized(wrapper) = self {
            Python::attach(|py| {
                if let Err(e) = wrapper
                    .0
                    .call_method1(py, pyo3::intern!(py, "teardown"), ())
                {
                    eprintln!("Error tearing down limit counter actor: {:?}", e);
                }
            });
        }
    }
}

pub(crate) struct LimitNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    limit: u64,
    offset: Option<u64>,
    child: DistributedPipelineNode,
}

impl LimitNode {
    const NODE_NAME: &'static str = "Limit";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        limit: usize,
        offset: Option<usize>,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::Limit,
            NodeCategory::StreamingSink,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            limit: limit as u64,
            offset: offset.map(|o| o as u64),
            child,
        }
    }

    #[cfg(feature = "python")]
    async fn execution_loop_fused(
        self: Arc<Self>,
        mut input_task_stream: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        let mut limit_actor = LimitActor::Uninitialized {
            limit: self.limit,
            offset: self.offset.unwrap_or(0),
        };

        let mut running_tasks = JoinSet::new();
        {
            // Scope `result_tx` so it gets dropped as soon as forwarding is done.
            // Downstream nodes that batch their input (e.g. `IntoPartitionsNode`)
            // wait for the input stream to close before submitting any task; if
            // we hold `result_tx` open while waiting for forwarded tasks to
            // complete, the forwarded task never gets submitted and we deadlock.
            let result_tx = result_tx;
            // Stamp each forwarded builder with a unique fingerprint suffix.
            // The worker caches local pipelines by `plan_fingerprint`; if all
            // tasks share a fingerprint, they share one pipeline — and our
            // streaming sink's state becomes Finished after the first task
            // hits the limit, killing the pipeline for subsequent tasks. Each
            // task must get its own pipeline.
            let mut seq: u32 = 0;
            while let Some(builder) = input_task_stream.next().await {
                let actor = limit_actor
                    .get_or_start(self.config.execution_config.actor_udf_ready_timeout)
                    .await?;
                let modified_builder = self
                    .append_distributed_limit_to_builder(builder, actor)
                    .extend_fingerprint(seq);
                seq = seq.wrapping_add(1);
                let (builder_with_token, notify_token) = modified_builder.add_notify_token();
                running_tasks.spawn(notify_token);
                if result_tx.send(builder_with_token).await.is_err() {
                    break;
                }
            }
            // result_tx dropped here at end-of-scope.
        }
        // Drain all forwarded tasks' notify channels before tearing down the
        // actor. Don't break on a `RecvError` from a notify channel — a
        // `SubmittedTask` that's dropped without being polled closes the
        // channel (Err), but the worker may still be making actor calls until
        // its cancellation propagates. Wait for every channel to drain.
        while running_tasks.join_next().await.is_some() {}
        limit_actor.teardown();
        Ok(())
    }

    #[cfg(feature = "python")]
    fn append_distributed_limit_to_builder(
        self: &Arc<Self>,
        builder: SwordfishTaskBuilder,
        actor: PyObjectWrapper,
    ) -> SwordfishTaskBuilder {
        let limit = self.limit;
        let offset = self.offset;
        let node_id = self.node_id();
        builder.map_plan(self.as_ref(), move |input| {
            LocalPhysicalPlan::distributed_limit(
                input,
                actor.clone(),
                limit,
                offset,
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            )
        })
    }
}

impl PipelineNodeImpl for LimitNode {
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
        match &self.offset {
            Some(o) => vec![format!("Limit: Num Rows = {}, Offset = {}", self.limit, o)],
            None => vec![format!("Limit: {}", self.limit)],
        }
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_stream = self.child.clone().produce_tasks(plan_context);

        #[cfg(feature = "python")]
        {
            let (result_tx, result_rx) = create_channel(1);
            plan_context.spawn(self.execution_loop_fused(input_stream, result_tx));
            TaskBuilderStream::from(result_rx)
        }
        #[cfg(not(feature = "python"))]
        {
            let _ = input_stream;
            unimplemented!("Distributed Limit requires the python feature")
        }
    }
}
