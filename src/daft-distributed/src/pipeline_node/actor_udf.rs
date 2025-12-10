use std::sync::Arc;

use common_error::DaftResult;
use common_py_serde::PyObjectWrapper;
use daft_dsl::{expr::bound_expr::BoundExpr, functions::python::UDFProperties, python::PyExpr};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;
use pyo3::{Py, PyAny, Python, types::PyAnyMethods};

use super::{
    NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl,
    SubmittableTaskStream,
};
use crate::{
    pipeline_node::{DistributedPipelineNode, append_plan_to_existing_task},
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::{scheduler::SubmittableTask, task::SwordfishTask},
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
        let py_exprs = projection
            .iter()
            .map(|e| PyExpr {
                expr: e.inner().clone(),
            })
            .collect::<Vec<_>>();
        let num_actors = udf_properties
            .concurrency
            .expect("ActorUDF should have concurrency specified");
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
                        py_exprs,
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
    projection: Vec<BoundExpr>,
    udf_properties: UDFProperties,
    actor_ready_timeout: usize,
}

impl ActorUDF {
    const NODE_NAME: NodeName = "ActorUDF";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        projection: Vec<BoundExpr>,
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
        Ok(Self {
            config,
            context,
            child,
            projection,
            udf_properties,
            actor_ready_timeout: plan_config.config.actor_udf_ready_timeout,
        })
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
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
            .as_ref()
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);

        let batch_size = self.udf_properties.batch_size;
        let schema = self.config.schema.clone();
        let node_id = self.node_id();
        append_plan_to_existing_task(
            submittable_task,
            &(self.clone() as Arc<dyn PipelineNodeImpl>),
            &move |input| {
                LocalPhysicalPlan::distributed_actor_pool_project(
                    input,
                    actors.clone(),
                    batch_size,
                    memory_request,
                    schema.clone(),
                    StatsState::NotMaterialized,
                    LocalNodeContext {
                        origin_node_id: Some(node_id as usize),
                        additional: None,
                    },
                )
            },
        )
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
            format!(
                "Projection = [{}]",
                self.projection.iter().map(|e| e.to_string()).join(", ")
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

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop_fused(input_node, result_tx);
        plan_context.spawn(execution_loop);

        SubmittableTaskStream::from(result_rx)
    }
}
