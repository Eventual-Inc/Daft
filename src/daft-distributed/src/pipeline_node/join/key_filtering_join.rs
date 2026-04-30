use std::sync::Arc;

use daft_common::error::{DaftError, DaftResult};
use daft_common::metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use daft_common::py_serde::PyObjectWrapper;
use daft_common::runtime::{JoinSet, python::execute_python_coroutine};
use daft_dsl::{
    ExprRef, expr::bound_expr::BoundExpr, functions::python::UDFProperties, python::PyExpr,
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{ops::KeyFilteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, TryStreamExt};
use pyo3::{Py, PyAny, Python, types::PyAnyMethods};

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl,
        TaskBuilderStream,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::stats::{DefaultRuntimeStats, RuntimeStatsRef},
    utils::channel::{Sender, create_channel},
};

/// Result of calling the Python ``create_key_filter`` coroutine.
///
/// Contains the ingest UDF expression (to append to right-side tasks),
/// the filter UDF expression (to append to left-side tasks), and
/// opaque Python handles that must be kept alive for teardown.
struct KeyFilterResources {
    /// UDF expression that ingests keys into the actors when evaluated.
    ingest_expr: BoundExpr,
    /// UDF properties for the ingest expression (needed for async UDF path).
    ingest_udf_properties: UDFProperties,
    /// UDF expression that filters rows against the actors when evaluated.
    filter_expr: BoundExpr,
    /// Actor handles (Python list), kept alive for teardown.
    actor_handles: PyObjectWrapper,
    /// Placement group (Python object), kept alive for teardown.
    placement_group: PyObjectWrapper,
}

impl KeyFilterResources {
    /// Create Ray actors and build the ingest/filter UDF expressions.
    ///
    /// Calls Python's ``create_key_filter(config)`` via ``execute_python_coroutine``.
    /// The Python side only creates actors and builds expressions — no data loading.
    async fn create(
        config: &KeyFilteringConfig,
        right_schema: &SchemaRef,
        left_schema: &SchemaRef,
    ) -> DaftResult<Self> {
        let config_clone = config.clone();
        let right_schema = right_schema.clone();
        let left_schema = left_schema.clone();

        let result: Vec<Py<PyAny>> = execute_python_coroutine::<_, Vec<Py<PyAny>>>(move |py| {
            let module = py.import(pyo3::intern!(
                py,
                "daft.execution.key_filtering_join.bridge"
            ))?;
            let py_config: daft_logical_plan::ops::PyKeyFilteringConfig = config_clone.into();
            module.call_method1(pyo3::intern!(py, "create_key_filter"), (py_config,))
        })
        .await?;

        if result.len() != 4 {
            return Err(DaftError::InternalError(format!(
                "create_key_filter returned {} elements, expected 4",
                result.len()
            )));
        }

        let (ingest_ref, filter_ref, actors, pg) = Python::attach(
            |py| -> DaftResult<(ExprRef, ExprRef, Py<PyAny>, Py<PyAny>)> {
                let ingest_py_expr: PyExpr = result[0].extract(py)?;
                let filter_py_expr: PyExpr = result[1].extract(py)?;
                let actors = result[2].clone_ref(py);
                let pg = result[3].clone_ref(py);
                Ok((ingest_py_expr.expr, filter_py_expr.expr, actors, pg))
            },
        )?;

        let ingest_expr = BoundExpr::try_new(ingest_ref.clone(), &right_schema)?;
        let ingest_udf_properties = UDFProperties::from_expr(&ingest_ref)?;
        let filter_expr = BoundExpr::try_new(filter_ref, &left_schema)?;

        Ok(Self {
            ingest_expr,
            ingest_udf_properties,
            filter_expr,
            actor_handles: PyObjectWrapper(Arc::new(actors)),
            placement_group: PyObjectWrapper(Arc::new(pg)),
        })
    }

    /// Teardown actors and cleanup placement group.
    fn teardown(&self) {
        Python::attach(|py| {
            let module = py.import(pyo3::intern!(
                py,
                "daft.execution.key_filtering_join.bridge"
            ));
            if let Ok(module) = module
                && let Err(e) = module.call_method1(
                    pyo3::intern!(py, "teardown_key_filter"),
                    (
                        self.actor_handles.0.as_ref().clone_ref(py),
                        self.placement_group.0.as_ref().clone_ref(py),
                    ),
                )
            {
                tracing::error!("Error tearing down key filter actors: {:?}", e);
            }
        });
    }
}

/// Pipeline node for `JoinStrategy::KeyFiltering`.
///
/// Two-child node (like `BroadcastJoinNode`):
/// - `right_child`: the key source; tasks get an ingest-UDF appended and are
///   fully materialised through the scheduler before the left side starts.
/// - `left_child`: the data to filter; each task gets a filter-UDF appended.
///
/// This avoids the nested `df.collect()` problem of the old single-child
/// design: both sides share the parent query's `query_idx` and are scheduled
/// by the same scheduler, so there are no cross-process ID conflicts.
pub(crate) struct KeyFilteringJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    left_child: DistributedPipelineNode,
    right_child: DistributedPipelineNode,
    right_schema: SchemaRef,
    key_filtering_config: KeyFilteringConfig,
    filter_batch_size: Option<usize>,
}

impl KeyFilteringJoinNode {
    const NODE_NAME: &'static str = "KeyFilteringJoin";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        key_filtering_config: KeyFilteringConfig,
        schema: SchemaRef,
        left_child: DistributedPipelineNode,
        right_child: DistributedPipelineNode,
    ) -> Self {
        let filter_batch_size = key_filtering_config.filter_batch_size;
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::KeyFilteringJoin,
            NodeCategory::BlockingSink,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            left_child.config().clustering_spec.clone(),
        );
        let right_schema = right_child.config().schema.clone();
        Self {
            config,
            context,
            left_child,
            right_child,
            right_schema,
            key_filtering_config,
            filter_batch_size,
        }
    }

    pub fn into_node(self, meter: &Meter) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self), meter)
    }

    async fn execution_loop(
        self: Arc<Self>,
        right_input: TaskBuilderStream,
        mut left_input: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Phase 1: Create Ray actors and build UDF expressions.
        let resources = KeyFilterResources::create(
            &self.key_filtering_config,
            &self.right_schema,
            &self.config.schema,
        )
        .await?;

        // Phase 2: Materialise right-side (key source) with ingest UDF.
        //
        // Each right-side task gets the ingest UDF appended via udf_project
        // so the async UDF can run without blocking the swordfish worker.
        // Keys are hash-routed to actors on the workers — data never
        // travels to head.
        let ingest_expr = resources.ingest_expr.clone();
        let ingest_udf_properties = resources.ingest_udf_properties.clone();
        let node_id = self.node_id();
        let keys_load_batch_size = self.key_filtering_config.keys_load_batch_size;
        let ingest_output_schema: SchemaRef = Arc::new(daft_schema::schema::Schema::new(vec![
            ingest_expr.inner().to_field(&self.right_schema)?,
        ]));
        let right_with_ingest = right_input.pipeline_instruction(
            self.clone() as Arc<dyn PipelineNodeImpl>,
            move |input| {
                // Optionally batch the right side for ingest
                let batched = if let Some(batch_size) = keys_load_batch_size {
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
                // Append ingest as a UDFProject so async UDFs are handled
                // by the AsyncUdfSink path, not blocking the worker thread.
                LocalPhysicalPlan::udf_project(
                    batched,
                    ingest_expr.clone(),
                    ingest_udf_properties.clone(),
                    vec![], // no passthrough columns — we discard the output
                    ingest_output_schema.clone(),
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(node_id as usize)),
                )
            },
        );

        // Materialise all right-side tasks through the scheduler.
        // We don't need the output data — only the side-effect of key ingestion.
        right_with_ingest
            .materialize(scheduler_handle, self.context.query_idx, task_id_counter)
            .try_collect::<Vec<_>>()
            .await?;

        // Phase 3: Stream left-side with filter UDF.
        let filter_predicate = resources.filter_expr.clone();
        let mut running_tasks = JoinSet::new();

        while let Some(builder) = left_input.next().await {
            let modified_builder =
                self.append_key_filter_to_builder(builder, filter_predicate.clone());

            let (builder_with_token, notify_token) = modified_builder.add_notify_token();
            running_tasks.spawn(notify_token);
            if result_tx.send(builder_with_token).await.is_err() {
                break;
            }
        }

        // Wait for all left-side tasks to finish.
        let mut first_error = None;
        while let Some(result) = running_tasks.join_next().await {
            match result? {
                Ok(()) => {}
                Err(err) if first_error.is_none() => first_error = Some(err),
                Err(_) => {}
            }
        }

        // Teardown actors after all tasks are finished.
        resources.teardown();

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
        vec![self.right_child.clone(), self.left_child.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        vec![
            format!(
                "KeyFilteringJoin: left_on={:?}",
                self.key_filtering_config.left_key_columns
            ),
            format!(
                "  right_on={:?}",
                self.key_filtering_config.right_key_columns
            ),
        ]
    }

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(DefaultRuntimeStats::new(meter, self.context()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let right_input = self.right_child.clone().produce_tasks(plan_context);
        let left_input = self.left_child.clone().produce_tasks(plan_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            right_input,
            left_input,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        );
        plan_context.spawn(execution_loop);

        TaskBuilderStream::from(result_rx)
    }
}
