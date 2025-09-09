use std::sync::Arc;

use common_error::DaftResult;
use common_py_serde::PyObjectWrapper;
use daft_dsl::{expr::bound_expr::BoundExpr, functions::python::UDFProperties, python::PyExpr};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;
use pyo3::{PyObject, Python, types::PyAnyMethods};

use super::{
    DisplayLevel, DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig,
    PipelineNodeContext, SubmittableTaskStream, TreeDisplay,
};
use crate::{
    pipeline_node::append_plan_to_existing_task,
    scheduling::{scheduler::SubmittableTask, task::SwordfishTask},
    stage::{StageConfig, StageExecutionContext},
    utils::{
        channel::{Sender, create_channel},
        joinset::JoinSet,
    },
};

#[derive(Debug)]

enum UDFActors {
    Uninitialized(Vec<BoundExpr>, UDFProperties),
    Initialized { actors: Vec<PyObjectWrapper> },
}

impl UDFActors {
    // TODO: This is a blocking call, and should be done asynchronously.
    async fn initialize_actors(
        projection: &[BoundExpr],
        udf_properties: &UDFProperties,
        actor_ready_timeout: usize,
    ) -> DaftResult<Vec<PyObjectWrapper>> {
        let (task_locals, py_exprs) = Python::with_gil(|py| {
            let task_locals = crate::utils::runtime::PYO3_ASYNC_RUNTIME_LOCALS
                .get()
                .expect("Python task locals not initialized")
                .clone_ref(py);
            let py_exprs = projection
                .iter()
                .map(|e| PyExpr {
                    expr: e.inner().clone(),
                })
                .collect::<Vec<_>>();
            (task_locals, py_exprs)
        });
        let num_actors = udf_properties
            .concurrency
            .expect("ActorUDF should have concurrency specified");
        let (gpu_request, cpu_request, memory_request) =
            match udf_properties.resource_request.clone() {
                Some(resource_request) => (
                    resource_request.num_gpus().unwrap_or(0.0),
                    resource_request.num_cpus().unwrap_or(0.0),
                    resource_request.memory_bytes().unwrap_or(0),
                ),
                None => (0.0, 0.0, 0),
            };

        // Use async pattern similar to DistributedActorPoolProjectOperator
        let await_coroutine = async move {
            let result = Python::with_gil(|py| {
                let ray_actor_pool_udf_module =
                    py.import(pyo3::intern!(py, "daft.execution.ray_actor_pool_udf"))?;
                let coroutine = ray_actor_pool_udf_module.call_method1(
                    pyo3::intern!(py, "start_udf_actors"),
                    (
                        py_exprs,
                        num_actors,
                        gpu_request,
                        cpu_request,
                        memory_request,
                        actor_ready_timeout,
                    ),
                )?;
                pyo3_async_runtimes::tokio::into_future(coroutine)
            })?
            .await?;
            DaftResult::Ok(result)
        };

        // Execute the coroutine with proper task locals
        let result = pyo3_async_runtimes::tokio::scope(task_locals, await_coroutine).await?;
        let actors = Python::with_gil(|py| {
            result.extract::<Vec<PyObject>>(py).map(|py_objects| {
                py_objects
                    .into_iter()
                    .map(|py_object| PyObjectWrapper(Arc::new(py_object)))
                    .collect::<Vec<_>>()
            })
        })?;
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
        Python::with_gil(|py| {
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
    child: Arc<dyn DistributedPipelineNode>,
    projection: Vec<BoundExpr>,
    udf_properties: UDFProperties,
    actor_ready_timeout: usize,
}

impl ActorUDF {
    const NODE_NAME: NodeName = "ActorUDF";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        projection: Vec<BoundExpr>,
        udf_properties: UDFProperties,
        schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> DaftResult<Self> {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id()],
            vec![child.name()],
            logical_node_id,
        );
        let config = PipelineNodeConfig::new(
            schema,
            stage_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Ok(Self {
            config,
            context,
            child,
            projection,
            udf_properties,
            actor_ready_timeout: stage_config.config.actor_udf_ready_timeout,
        })
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    async fn execution_loop_fused(
        self: Arc<Self>,
        mut input_task_stream: SubmittableTaskStream,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
    ) -> DaftResult<()> {
        let mut udf_actors =
            UDFActors::Uninitialized(self.projection.clone(), self.udf_properties.clone());

        let mut running_tasks = JoinSet::new();
        while let Some(task) = input_task_stream.next().await {
            let actors = udf_actors.get_actors(self.actor_ready_timeout).await?;

            let modified_task = self.append_actor_udf_to_task(task, actors);
            let (submittable_task, notify_token) = modified_task.add_notify_token();
            running_tasks.spawn(notify_token);
            if result_tx.send(submittable_task).await.is_err() {
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

    fn append_actor_udf_to_task(
        self: &Arc<Self>,
        submittable_task: SubmittableTask<SwordfishTask>,
        actors: Vec<PyObjectWrapper>,
    ) -> SubmittableTask<SwordfishTask> {
        let memory_request = self
            .udf_properties
            .resource_request
            .clone()
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);

        let batch_size = self.udf_properties.batch_size;
        let schema = self.config.schema.clone();
        append_plan_to_existing_task(
            submittable_task,
            &(self.clone() as Arc<dyn DistributedPipelineNode>),
            &move |input| {
                LocalPhysicalPlan::distributed_actor_pool_project(
                    input,
                    actors.clone(),
                    batch_size,
                    memory_request,
                    schema.clone(),
                    StatsState::NotMaterialized,
                )
            },
        )
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec![];
        res.push("ActorUDF:".to_string());
        res.push(format!(
            "Projection = [{}]",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("UDF = {}", self.udf_properties.name));
        res.push(format!(
            "Concurrency = {}",
            self.udf_properties
                .concurrency
                .expect("ActorUDF should have concurrency specified")
        ));
        if let Some(resource_request) = self.udf_properties.resource_request.clone() {
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
}

impl DistributedPipelineNode for ActorUDF {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(stage_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop_fused(input_node, result_tx);
        stage_context.spawn(execution_loop);

        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

impl TreeDisplay for ActorUDF {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.name()).unwrap();
            }
            _ => {
                let multiline_display = self.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.name().to_string()
    }
}
