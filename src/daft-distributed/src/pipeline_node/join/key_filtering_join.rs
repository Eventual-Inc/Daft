use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::ops::{NodeCategory, NodeType};
use common_py_serde::PyObjectWrapper;
use common_runtime::{JoinSet, python::execute_python_coroutine};
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr, python::PyExpr};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{ops::SkipExistingSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;
use opentelemetry::metrics::Meter;
use pyo3::{Py, PyAny, Python, types::PyAnyMethods};

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl,
        TaskBuilderStream,
    },
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    statistics::stats::{DefaultRuntimeStats, RuntimeStatsRef},
    utils::channel::{Sender, create_channel},
};

/// State machine for lazy key-filter actor initialization.
///
/// Mirrors the `UDFActors` pattern from `actor_udf.rs`:
/// actors are NOT created during node construction —
/// they are lazily initialized on the first partition and reused across all batches.
enum KeyFilterActors {
    /// Actors have not been created yet; holds the spec for deferred initialization.
    Uninitialized(Box<SkipExistingSpec>),
    /// Actors are running and ready to filter.
    Initialized {
        /// The resolved filter predicate expression (an async-batch UDF that calls actors).
        filter_predicate: BoundExpr,
        /// Actor handles (Python list), kept alive for teardown.
        actor_handles: PyObjectWrapper,
        /// Placement group (Python object or None), kept alive for teardown.
        placement_group: PyObjectWrapper,
    },
    /// No existing data found; all rows pass through (no filtering needed).
    NoExistingData,
}

impl KeyFilterActors {
    /// Initialize actors lazily by calling Python's
    /// `daft.execution.key_filtering_join.bridge.initialize_key_filter`.
    ///
    /// Uses `execute_python_coroutine` so that the blocking Python code
    /// (which internally calls `df.collect()` → Rust runtime) runs on the
    /// asyncio event loop thread instead of a tokio worker thread, avoiding
    /// "Cannot start a runtime from within a runtime" panics.
    ///
    /// Returns the new state.
    async fn initialize(spec: &SkipExistingSpec, input_schema: &SchemaRef) -> DaftResult<Self> {
        let spec_clone = spec.clone();
        let input_schema = input_schema.clone();

        // Call the async Python `initialize_key_filter(spec)` coroutine.
        // It internally delegates blocking work to `asyncio.to_thread` so
        // the tokio runtime is never re-entered.
        //
        // Python returns a list: [] if no existing data, or
        // [filter_expr, actor_handles, placement_group].
        // Vec<Py<PyAny>> satisfies the FromPyObject trait bounds.
        let result: Vec<Py<PyAny>> = execute_python_coroutine::<_, Vec<Py<PyAny>>>(move |py| {
            let skip_existing_module = py.import(pyo3::intern!(
                py,
                "daft.execution.key_filtering_join.bridge"
            ))?;
            let py_spec: daft_logical_plan::ops::PySkipExistingSpec = spec_clone.into();
            skip_existing_module
                .call_method1(pyo3::intern!(py, "initialize_key_filter"), (py_spec,))
        })
        .await?;

        // Extract the result components.
        if result.is_empty() {
            return Ok(Self::NoExistingData);
        }

        let extracted = Python::attach(|py| -> DaftResult<(ExprRef, Py<PyAny>, Py<PyAny>)> {
            let py_expr: PyExpr = result[0].extract(py)?;
            let actors = result[1].clone_ref(py);
            let pg = result[2].clone_ref(py);
            Ok((py_expr.expr, actors, pg))
        })?;

        let (expr_ref, actors, pg) = extracted;
        let bound = BoundExpr::try_new(expr_ref, &input_schema)?;
        Ok(Self::Initialized {
            filter_predicate: bound,
            actor_handles: PyObjectWrapper(Arc::new(actors)),
            placement_group: PyObjectWrapper(Arc::new(pg)),
        })
    }

    /// Ensure actors are initialized. Returns the filter predicate (or None if no existing data).
    async fn get_filter_predicate(
        &mut self,
        input_schema: &SchemaRef,
    ) -> DaftResult<Option<BoundExpr>> {
        if let Self::Uninitialized(spec) = self {
            let new_state = Self::initialize(spec, input_schema).await?;
            *self = new_state;
        }
        match self {
            Self::Initialized {
                filter_predicate, ..
            } => Ok(Some(filter_predicate.clone())),
            Self::NoExistingData => Ok(None),
            Self::Uninitialized(_) => unreachable!(),
        }
    }

    /// Teardown actors and cleanup placement group.
    fn teardown(&mut self) {
        Python::attach(|py| {
            if let Self::Initialized {
                actor_handles,
                placement_group,
                ..
            } = self
            {
                let module = py.import(pyo3::intern!(
                    py,
                    "daft.execution.key_filtering_join.bridge"
                ));
                if let Ok(module) = module
                    && let Err(e) = module.call_method1(
                        pyo3::intern!(py, "teardown_key_filter"),
                        (
                            actor_handles.0.as_ref().clone_ref(py),
                            placement_group.0.as_ref().clone_ref(py),
                        ),
                    )
                {
                    tracing::error!("Error tearing down key filter actors: {:?}", e);
                }
            }
        });
    }
}

/// Pipeline node for `JoinStrategy::KeyFiltering`.
///
/// This is the physical execution node for skip_existing anti-joins.
/// Like `ActorUDF`, it lazily creates Ray actors when the first partition arrives,
/// then filters each partition through the actors.
///
/// Only has a single child (the left/input side of the anti-join);
/// the right side (PlaceHolder source) is replaced by live Ray actors.
pub(crate) struct KeyFilteringJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    child: DistributedPipelineNode,
    spec: SkipExistingSpec,
    filter_batch_size: Option<usize>,
}

impl KeyFilteringJoinNode {
    const NODE_NAME: &'static str = "KeyFilteringJoin";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        spec: SkipExistingSpec,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let filter_batch_size = spec.filter_batch_size;
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::KeyFilteringJoin,
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
            child,
            spec,
            filter_batch_size,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    async fn execution_loop(
        self: Arc<Self>,
        mut input_task_stream: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        let mut actors = KeyFilterActors::Uninitialized(Box::new(self.spec.clone()));

        let mut running_tasks = JoinSet::new();
        while let Some(builder) = input_task_stream.next().await {
            let filter_predicate = actors.get_filter_predicate(&self.config.schema).await?;

            let modified_builder = match filter_predicate {
                Some(predicate) => self.append_key_filter_to_builder(builder, predicate),
                None => {
                    // No existing data — pass through without filtering
                    builder
                }
            };

            let (builder_with_token, notify_token) = modified_builder.add_notify_token();
            running_tasks.spawn(notify_token);
            if result_tx.send(builder_with_token).await.is_err() {
                break;
            }
        }
        // Wait for all tasks to finish.
        let mut first_error = None;
        while let Some(result) = running_tasks.join_next().await {
            match result? {
                Ok(()) => {}
                Err(err) if first_error.is_none() => first_error = Some(err),
                Err(_) => {}
            }
        }
        // Only teardown actors after all tasks are finished.
        actors.teardown();
        if let Some(err) = first_error {
            return Err(DaftError::InternalError(format!(
                "Sender of OneShot Channel dropped before sending task completion notification: {err}"
            )));
        }
        Ok(())
    }

    fn append_key_filter_to_builder(
        self: &Arc<Self>,
        builder: SwordfishTaskBuilder,
        filter_predicate: BoundExpr,
    ) -> SwordfishTaskBuilder {
        let node_id = self.node_id();
        let filter_batch_size = self.filter_batch_size;

        builder.map_plan(self.as_ref(), move |input| {
            // Optionally wrap with IntoBatches if filter_batch_size is set
            let filter_input = if let Some(batch_size) = filter_batch_size {
                LocalPhysicalPlan::into_batches(
                    input,
                    batch_size,
                    false,
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(node_id as usize)),
                )
            } else {
                input
            };
            // Apply the filter predicate (async-batch UDF that calls key filter actors)
            LocalPhysicalPlan::filter(
                filter_input,
                filter_predicate.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            )
        })
    }
}

impl PipelineNodeImpl for KeyFilteringJoinNode {
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
        vec![
            format!("KeyFilteringJoin: keys={:?}", self.spec.key_column),
            format!("  existing_path={:?}", self.spec.existing_path),
            format!("  file_format={:?}", self.spec.file_format),
        ]
    }

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(DefaultRuntimeStats::new(meter, self.context()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(input_node, result_tx);
        plan_context.spawn(execution_loop);

        TaskBuilderStream::from(result_rx)
    }
}
