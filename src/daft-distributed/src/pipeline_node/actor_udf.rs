use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use common_py_serde::PyObjectWrapper;
use common_runtime::JoinSet;
use daft_dsl::{
    expr::bound_expr::BoundExpr, functions::python::UDFProperties, python::PyExpr,
    utils::remap_used_cols,
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;
use pyo3::{Py, PyAny, Python, types::PyAnyMethods};

use super::{
    ClusteringStrategy, NodeID, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl,
    udf::UdfStats,
};
use crate::{
    pipeline_node::{DistributedPipelineNode, TaskBuilderStream},
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::{SwordfishTaskBuilder, TaskID},
    statistics::stats::RuntimeStatsRef,
    utils::channel::{Sender, create_channel},
};

#[derive(Debug)]
enum UDFActors {
    Uninitialized(BoundExpr, UDFProperties),
    Initialized { pool: PyObjectWrapper },
}

struct UDFActorHandleLease {
    pool: PyObjectWrapper,
}

impl Drop for UDFActorHandleLease {
    fn drop(&mut self) {
        UDFActors::release_actor_handles(self.pool.clone());
    }
}

impl UDFActors {
    // TODO: This is a blocking call, and should be done asynchronously.
    async fn initialize_actor_pool(
        udf_expr: &BoundExpr,
        udf_properties: &UDFProperties,
        actor_ready_timeout: usize,
    ) -> DaftResult<PyObjectWrapper> {
        let py_expr = PyExpr {
            expr: udf_expr.inner().clone(),
        };
        let num_actors = udf_properties
            .concurrency
            .expect("ActorUDF should have concurrency specified");
        let min_actors = udf_properties.min_concurrency.unwrap_or(num_actors);
        let (gpu_request, cpu_request, memory_request) = match &udf_properties.resource_request {
            Some(resource_request) => (
                resource_request.num_gpus().unwrap_or(0.0),
                resource_request.num_cpus().unwrap_or(1.0),
                resource_request.memory_bytes().unwrap_or(0),
            ),
            None => (0.0, 1.0, 0),
        };

        let actor_name = udf_properties.name.clone();
        let ray_options = udf_properties.ray_options.clone();
        let result = common_runtime::python::execute_python_coroutine::<_, Py<PyAny>>(move |py| {
            let ray_actor_pool_udf_module =
                py.import(pyo3::intern!(py, "daft.execution.ray_actor_pool_udf"))?;
            // Convert RuntimePyObject option to a Python object (dict) or None
            let py_ray_options = match &ray_options {
                Some(ro) => ro.as_ref().clone_ref(py),
                None => py.None(),
            };
            ray_actor_pool_udf_module.call_method1(
                pyo3::intern!(py, "start_udf_actor_pool"),
                (
                    py_expr,
                    min_actors.get(),
                    num_actors.get(),
                    gpu_request,
                    cpu_request,
                    memory_request,
                    py_ray_options,
                    actor_ready_timeout,
                    actor_name,
                ),
            )
        })
        .await?;

        Ok(PyObjectWrapper(Arc::new(result)))
    }

    async fn get_actors(
        &mut self,
        actor_ready_timeout: usize,
        pending_tasks: usize,
    ) -> DaftResult<Vec<PyObjectWrapper>> {
        match self {
            Self::Uninitialized(projection, udf_properties) => {
                let pool =
                    Self::initialize_actor_pool(projection, udf_properties, actor_ready_timeout)
                        .await?;
                let actors = Self::get_actors_from_pool(pool.clone(), pending_tasks).await?;
                *self = Self::Initialized { pool };
                Ok(actors)
            }
            Self::Initialized { pool } => {
                Self::get_actors_from_pool(pool.clone(), pending_tasks).await
            }
        }
    }

    async fn get_leased_actors(
        &mut self,
        actor_ready_timeout: usize,
        pending_tasks: usize,
    ) -> DaftResult<(Vec<PyObjectWrapper>, UDFActorHandleLease)> {
        match self {
            Self::Uninitialized(projection, udf_properties) => {
                let pool =
                    Self::initialize_actor_pool(projection, udf_properties, actor_ready_timeout)
                        .await?;
                let actors = Self::get_leased_actors_from_pool(pool.clone(), pending_tasks).await?;
                let lease = UDFActorHandleLease { pool: pool.clone() };
                *self = Self::Initialized { pool };
                Ok((actors, lease))
            }
            Self::Initialized { pool } => {
                let actors = Self::get_leased_actors_from_pool(pool.clone(), pending_tasks).await?;
                let lease = UDFActorHandleLease { pool: pool.clone() };
                Ok((actors, lease))
            }
        }
    }

    async fn get_actors_from_pool(
        pool: PyObjectWrapper,
        pending_tasks: usize,
    ) -> DaftResult<Vec<PyObjectWrapper>> {
        Self::call_actor_handles_method(pool, pending_tasks, "get_actor_handles").await
    }

    async fn get_leased_actors_from_pool(
        pool: PyObjectWrapper,
        pending_tasks: usize,
    ) -> DaftResult<Vec<PyObjectWrapper>> {
        Self::call_actor_handles_method(pool, pending_tasks, "get_leased_actor_handles").await
    }

    async fn call_actor_handles_method(
        pool: PyObjectWrapper,
        pending_tasks: usize,
        method_name: &'static str,
    ) -> DaftResult<Vec<PyObjectWrapper>> {
        let result =
            common_runtime::python::execute_python_coroutine::<_, Vec<Py<PyAny>>>(move |py| {
                let coroutine = pool.0.call_method1(py, method_name, (pending_tasks,))?;
                Ok(coroutine.into_bound(py))
            })
            .await?;
        Ok(result
            .into_iter()
            .map(|py_object| PyObjectWrapper(Arc::new(py_object)))
            .collect::<Vec<_>>())
    }

    fn cleanup_retired_actors(&mut self) {
        Python::attach(|py| {
            if let Self::Initialized { pool, .. } = self
                && let Err(e) = pool
                    .0
                    .call_method0(py, pyo3::intern!(py, "cleanup_retired_actors"))
            {
                eprintln!("Error cleaning up retired UDF actors: {:?}", e);
            }
        });
    }

    fn release_actor_handles(pool: PyObjectWrapper) {
        Python::attach(|py| {
            if let Err(e) = pool
                .0
                .call_method0(py, pyo3::intern!(py, "release_actor_handles"))
            {
                eprintln!("Error releasing UDF actor handles: {:?}", e);
            }
        });
    }

    async fn scale_down_to_min_if_initialized(&mut self) -> DaftResult<()> {
        if let Self::Initialized { pool } = self {
            Self::get_actors_from_pool(pool.clone(), 0).await?;
        }
        Ok(())
    }

    fn teardown(&mut self) {
        Python::attach(|py| {
            if let Self::Initialized { pool, .. } = self
                && let Err(e) = pool.0.call_method0(py, pyo3::intern!(py, "teardown"))
            {
                eprintln!("Error tearing down actor pool: {:?}", e);
            }
        });
    }
}

impl Drop for UDFActors {
    fn drop(&mut self) {
        self.teardown();
    }
}

impl UDFActors {
    fn drain_completed_tasks(
        running_tasks: &mut JoinSet<Result<TaskID, tokio::sync::oneshot::error::RecvError>>,
    ) -> DaftResult<Option<bool>> {
        while let Some(result) = running_tasks.try_join_next() {
            if result?.is_err() {
                return Ok(Some(false));
            }
        }
        Ok(None)
    }
}

pub(crate) struct ActorUDF {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    child: DistributedPipelineNode,
    udf_expr: BoundExpr,
    passthrough_columns: Vec<BoundExpr>,
    required_columns: Vec<usize>,
    udf_properties: UDFProperties,
    actor_ready_timeout: usize,
}

impl ActorUDF {
    const NODE_NAME: &'static str = "ActorUDF";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        udf_expr: BoundExpr,
        passthrough_columns: Vec<BoundExpr>,
        udf_properties: UDFProperties,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> DaftResult<Self> {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::DistributedActorPoolProject,
            NodeCategory::Intermediate,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            ClusteringStrategy::Projection {
                child: &child,
                projection: &passthrough_columns,
            },
        );
        let (udf_expr, required_columns) = remap_used_cols(udf_expr);
        Ok(Self {
            config,
            context,
            child,
            udf_expr,
            passthrough_columns,
            required_columns,
            udf_properties,
            actor_ready_timeout: plan_config.config.actor_udf_ready_timeout,
        })
    }

    async fn execution_loop_fused(
        self: Arc<Self>,
        mut input_task_stream: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        let mut udf_actors =
            UDFActors::Uninitialized(self.udf_expr.clone(), self.udf_properties.clone());
        // Start each UDF's min_concurrency before consuming upstream input.
        // This gives downstream UDFs a chance to reserve their min actors before
        // upstream UDFs consume all cluster resources with opportunistic
        // above-min autoscaling.
        udf_actors.get_actors(self.actor_ready_timeout, 0).await?;

        let mut running_tasks = JoinSet::new();
        while let Some(builder) = input_task_stream.next().await {
            if matches!(
                UDFActors::drain_completed_tasks(&mut running_tasks)?,
                Some(false)
            ) {
                break;
            }
            if running_tasks.is_empty() {
                udf_actors.cleanup_retired_actors();
            }

            let pending_tasks = running_tasks.len() + 1;
            let (actors, lease) = udf_actors
                .get_leased_actors(self.actor_ready_timeout, pending_tasks)
                .await?;

            let modified_builder = self.append_actor_udf_to_builder(builder, actors);
            let (builder_with_token, notify_token) = modified_builder.add_notify_token();
            if result_tx.send(builder_with_token).await.is_err() {
                break;
            }
            running_tasks.spawn(async move {
                let result = notify_token.await;
                drop(lease);
                result
            });
        }
        // Drop the sender so downstream BlockingSinks observe EOF and flush;
        // otherwise their notify_tokens (awaited below) never fire -> deadlock.
        drop(result_tx);
        // Wait for all tasks to finish.
        while let Some(result) = running_tasks.join_next().await {
            if result?.is_err() {
                break;
            }
        }
        udf_actors.scale_down_to_min_if_initialized().await?;
        udf_actors.cleanup_retired_actors();
        // Only teardown actors after all tasks are finished.
        udf_actors.teardown();
        Ok(())
    }

    fn append_actor_udf_to_builder(
        self: &Arc<Self>,
        builder: SwordfishTaskBuilder,
        actors: Vec<PyObjectWrapper>,
    ) -> SwordfishTaskBuilder {
        let memory_request = self
            .udf_properties
            .resource_request
            .as_ref()
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);

        builder.map_plan(self.as_ref(), |input| {
            LocalPhysicalPlan::distributed_actor_pool_project(
                input,
                actors.clone(),
                self.udf_properties.batch_size,
                memory_request,
                self.config.schema.clone(),
                self.passthrough_columns.clone(),
                self.required_columns.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self.node_id() as usize)),
            )
        })
    }
}

impl PipelineNodeImpl for ActorUDF {
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
        let mut res = vec![
            format!("ActorUDF: {}", self.udf_properties.name),
            format!("Expr = {}", self.udf_expr),
            format!(
                "Passthrough Columns = [{}]",
                self.passthrough_columns.iter().join(", ")
            ),
            format!(
                "Properties = {{ {} }}",
                self.udf_properties.multiline_display(false).join(", ")
            ),
        ];

        if let Some(resource_request) = &self.udf_properties.resource_request {
            let multiline_display = resource_request.multiline_display();
            res.push(format!(
                "Resource request = {{ {} }}",
                multiline_display.join(", ")
            ));
        } else {
            res.push("Resource request = None".to_string());
        }

        res
    }

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(UdfStats::new(meter, self.context()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop_fused(input_node, result_tx);
        plan_context.spawn(execution_loop);

        TaskBuilderStream::from(result_rx)
    }
}
