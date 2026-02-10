use std::sync::Arc;

use common_error::DaftResult;
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
use opentelemetry::metrics::Meter;
use pyo3::{Py, PyAny, Python, types::PyAnyMethods};

use super::{
    NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl, udf::UdfStats,
};
use crate::{
    pipeline_node::{DistributedPipelineNode, TaskBuilderStream},
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    statistics::stats::RuntimeStatsRef,
    utils::channel::{Sender, create_channel},
};

#[derive(Debug)]
enum UDFActors {
    Uninitialized(BoundExpr, UDFProperties),
    Initialized { actors: Vec<PyObjectWrapper> },
}

impl UDFActors {
    // TODO: This is a blocking call, and should be done asynchronously.
    async fn initialize_actors(
        udf_expr: &BoundExpr,
        udf_properties: &UDFProperties,
        actor_ready_timeout: usize,
    ) -> DaftResult<Vec<PyObjectWrapper>> {
        let py_expr = PyExpr {
            expr: udf_expr.inner().clone(),
        };
        let num_actors = udf_properties
            .max_concurrency
            .expect("ActorUDF should have max_concurrency specified");
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
        let result =
            common_runtime::python::execute_python_coroutine::<_, Vec<Py<PyAny>>>(move |py| {
                let ray_actor_pool_udf_module =
                    py.import(pyo3::intern!(py, "daft.execution.ray_actor_pool_udf"))?;
                // Convert RuntimePyObject option to a Python object (dict) or None
                let py_ray_options = match &ray_options {
                    Some(ro) => ro.as_ref().clone_ref(py),
                    None => py.None(),
                };
                ray_actor_pool_udf_module.call_method1(
                    pyo3::intern!(py, "start_udf_actors"),
                    (
                        py_expr,
                        num_actors,
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

        let actors = result
            .into_iter()
            .map(|py_object| PyObjectWrapper(Arc::new(py_object)))
            .collect::<Vec<_>>();
        Ok(actors)
    }

    async fn get_actors(&mut self, actor_ready_timeout: usize) -> DaftResult<Vec<PyObjectWrapper>> {
        match self {
            Self::Uninitialized(projection, udf_properties) => {
                let actors =
                    Self::initialize_actors(projection, udf_properties, actor_ready_timeout)
                        .await?;
                *self = Self::Initialized {
                    actors: actors.clone(),
                };
                Ok(actors)
            }
            Self::Initialized { actors } => Ok(actors.clone()),
        }
    }

    fn teardown(&mut self) {
        Python::attach(|py| {
            if let Self::Initialized { actors, .. } = self {
                for actor in actors {
                    if let Err(e) = actor.0.call_method1(py, pyo3::intern!(py, "teardown"), ()) {
                        eprintln!("Error tearing down actor: {:?}", e);
                    }
                }
            }
        });
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
    const NODE_NAME: NodeName = "ActorUDF";

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
            Self::NODE_NAME,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
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

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    async fn execution_loop_fused(
        self: Arc<Self>,
        mut input_task_stream: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        let mut udf_actors =
            UDFActors::Uninitialized(self.udf_expr.clone(), self.udf_properties.clone());

        let mut running_tasks = JoinSet::new();
        while let Some(builder) = input_task_stream.next().await {
            let actors = udf_actors.get_actors(self.actor_ready_timeout).await?;

            let modified_builder = self.append_actor_udf_to_builder(builder, actors);
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
                self.udf_properties.min_concurrency,
                self.udf_properties.max_concurrency.map(|c| c.get()),
                memory_request,
                self.config.schema.clone(),
                self.passthrough_columns.clone(),
                self.required_columns.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(self.node_id() as usize),
                    additional: None,
                },
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

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(UdfStats::new(meter, self.node_id()))
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
