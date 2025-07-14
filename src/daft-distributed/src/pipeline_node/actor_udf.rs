use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_dsl::{
    expr::bound_expr::BoundExpr,
    functions::python::{get_concurrency, get_resource_request},
    pyobj_serde::PyObjectWrapper,
    python::PyExpr,
};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;
use pyo3::{types::PyAnyMethods, PyObject, Python};

use super::{
    DisplayLevel, DistributedPipelineNode, MaterializedOutput, NodeID, NodeName,
    PipelineNodeConfig, PipelineNodeContext, PipelineOutput, RunningPipelineNode, TreeDisplay,
};
use crate::{
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
        worker::WorkerId,
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::{
        channel::{create_channel, Sender},
        joinset::JoinSet,
    },
};

#[derive(Debug)]

enum UDFActors {
    Uninitialized(Vec<BoundExpr>),
    Initialized {
        actors: Vec<(WorkerId, Vec<PyObjectWrapper>)>,
        round_robin_counter: usize,
    },
}

impl UDFActors {
    // TODO: This is a blocking call, and should be done asynchronously.
    fn initialize_actors(
        projection: &[BoundExpr],
    ) -> DaftResult<Vec<(WorkerId, Vec<PyObjectWrapper>)>> {
        let num_actors = get_concurrency(projection);
        let (gpu_request, cpu_request, memory_request) = match get_resource_request(projection) {
            Some(resource_request) => (
                resource_request.num_gpus().unwrap_or(0.0),
                resource_request.num_cpus().unwrap_or(1.0),
                resource_request.memory_bytes().unwrap_or(0),
            ),
            None => (0.0, 1.0, 0),
        };

        let actors = Python::with_gil(|py| {
            let ray_actor_pool_udf_module =
                py.import(pyo3::intern!(py, "daft.execution.ray_actor_pool_udf"))?;
            let py_exprs = projection
                .iter()
                .map(|e| PyExpr {
                    expr: e.inner().clone(),
                })
                .collect::<Vec<_>>();
            let actors = ray_actor_pool_udf_module.call_method1(
                pyo3::intern!(py, "start_udf_actors"),
                (
                    py_exprs,
                    num_actors,
                    gpu_request,
                    cpu_request,
                    memory_request,
                ),
            )?;
            DaftResult::Ok(
                actors
                    .extract::<Vec<(String, Vec<PyObject>)>>()?
                    .into_iter()
                    .map(|(worker_id, py_objects)| {
                        (
                            worker_id.into(),
                            py_objects
                                .into_iter()
                                .map(|py_object| PyObjectWrapper(Arc::new(py_object)))
                                .collect(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        })?;
        Ok(actors)
    }

    fn get_actors_for_worker(&mut self, worker_id: &WorkerId) -> DaftResult<Vec<PyObjectWrapper>> {
        if let Self::Uninitialized(projection) = self {
            let actors = Self::initialize_actors(projection)?;
            *self = Self::Initialized {
                actors,
                round_robin_counter: 0,
            };
        }
        let Self::Initialized { actors, .. } = self else {
            panic!("ActorUDF::get_actors_for_worker: ActorUDF must be initialized");
        };

        let actors_for_worker = actors
            .iter()
            .find(|(id, _)| id == worker_id)
            .map(|(_, actors_for_worker)| actors_for_worker.clone())
            .unwrap_or_default();
        Ok(actors_for_worker)
    }

    fn get_round_robin_actors(&mut self) -> DaftResult<(WorkerId, Vec<PyObjectWrapper>)> {
        if let Self::Uninitialized(projection) = self {
            let actors = Self::initialize_actors(projection)?;
            *self = Self::Initialized {
                actors,
                round_robin_counter: 0,
            };
        }
        let Self::Initialized {
            actors,
            round_robin_counter,
        } = self
        else {
            panic!("ActorUDF::get_round_robin_actors: ActorUDF must be initialized");
        };

        let (next_worker_id, next_actors) = actors[*round_robin_counter].clone();
        *round_robin_counter = (*round_robin_counter + 1) % actors.len();
        Ok((next_worker_id, next_actors))
    }

    fn teardown(&mut self) {
        Python::with_gil(|py| {
            if let Self::Initialized { actors, .. } = self {
                for (_, actors) in actors {
                    for actor in actors {
                        if let Err(e) = actor.0.call_method1(py, pyo3::intern!(py, "teardown"), ())
                        {
                            eprintln!("Error tearing down actor: {:?}", e);
                        }
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
    batch_size: Option<usize>,
    memory_request: u64,
}

impl ActorUDF {
    const NODE_NAME: NodeName = "ActorUDF";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        projection: Vec<BoundExpr>,
        batch_size: Option<usize>,
        memory_request: u64,
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
            batch_size,
            memory_request,
        })
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    async fn execution_loop_fused(
        self: Arc<Self>,
        input: RunningPipelineNode,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        let mut udf_actors = UDFActors::Uninitialized(self.projection.clone());

        let mut materialized_output_stream = input.materialize_running();
        let mut running_tasks = JoinSet::new();
        while let Some(pipeline_output) = materialized_output_stream.next().await {
            let pipeline_output = pipeline_output?;
            match pipeline_output {
                PipelineOutput::Materialized(materialized_output) => {
                    let actors =
                        udf_actors.get_actors_for_worker(&materialized_output.worker_id)?;

                    // If no actors for this worker, pick actors using round robin
                    let (worker_id, actors) = if actors.is_empty() {
                        udf_actors.get_round_robin_actors()?
                    } else {
                        (materialized_output.worker_id.clone(), actors)
                    };

                    let task = self.make_actor_udf_task_for_materialized_outputs(
                        materialized_output,
                        worker_id,
                        actors,
                        TaskContext::from((&self.context, task_id_counter.next())),
                    )?;
                    let (submittable_task, notify_token) = task.add_notify_token();
                    running_tasks.spawn(notify_token);
                    if result_tx
                        .send(PipelineOutput::Task(submittable_task))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                PipelineOutput::Task(task) => {
                    // TODO: THIS IS NOT GOING TO WORK IF THE TASK HAS AN EXISTING SCHEDULING STRATEGY.
                    // I.E. THERE WAS A PREVIOUS ACTOR UDF. IF THERE IS A CASE WHERE THE PREVIOUS ACTOR UDF HAS A SPECIFIC WORKER ID,
                    // AND WE DON'T HAVE ACTOR FOR THIS WORKER ID, IT SHOULD STILL WORK BECAUSE IT'S A RAY ACTOR CALL.
                    // BUT WE INCUR TRANSFER COSTS FOR THE TASK.
                    // IN A CASE LIKE THIS WE SHOULD MATERIALIZE THE TASK.

                    // Pick actors using round robin
                    let (worker_id, actors) = udf_actors.get_round_robin_actors()?;

                    let modified_task = self.append_actor_udf_to_task(
                        worker_id,
                        task,
                        actors,
                        TaskContext::from((&self.context, task_id_counter.next())),
                    )?;
                    let (submittable_task, notify_token) = modified_task.add_notify_token();
                    running_tasks.spawn(notify_token);
                    if result_tx
                        .send(PipelineOutput::Task(submittable_task))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                PipelineOutput::Running(_) => unreachable!(),
            }
        }
        while let Some(result) = running_tasks.join_next().await {
            if result?.is_err() {
                break;
            }
        }
        // Only teardown actors after all tasks are finished.
        udf_actors.teardown();
        Ok(())
    }

    fn make_actor_udf_task_for_materialized_outputs(
        &self,
        materialized_output: MaterializedOutput,
        worker_id: WorkerId,
        actors: Vec<PyObjectWrapper>,
        task_context: TaskContext,
    ) -> DaftResult<SubmittableTask<SwordfishTask>> {
        // Extract all partitions from materialized outputs
        let partitions = materialized_output.partitions().to_vec();

        let in_memory_info = InMemoryInfo::new(
            self.config.schema.clone(),
            self.context.node_id.to_string(),
            None,
            partitions.len(),
            0,
            0,
            None,
            None,
        );

        let in_memory_source =
            LocalPhysicalPlan::in_memory_scan(in_memory_info, StatsState::NotMaterialized);

        let actor_pool_project_plan = LocalPhysicalPlan::distributed_actor_pool_project(
            in_memory_source,
            actors.into_iter().map(|e| e.into()).collect(),
            self.batch_size,
            self.memory_request,
            self.config.schema.clone(),
            StatsState::NotMaterialized,
        );
        let task = SubmittableTask::new(SwordfishTask::new(
            task_context,
            actor_pool_project_plan,
            self.config.execution_config.clone(),
            HashMap::from([(self.context.node_id.to_string(), partitions)]),
            SchedulingStrategy::WorkerAffinity {
                worker_id,
                soft: false,
            },
            self.context.to_hashmap(),
        ));

        Ok(task)
    }

    fn append_actor_udf_to_task(
        &self,
        worker_id: WorkerId,
        submittable_task: SubmittableTask<SwordfishTask>,
        actors: Vec<PyObjectWrapper>,
        task_context: TaskContext,
    ) -> DaftResult<SubmittableTask<SwordfishTask>> {
        let task_plan = submittable_task.task().plan();
        let actor_pool_project_plan = LocalPhysicalPlan::distributed_actor_pool_project(
            task_plan,
            actors,
            self.batch_size,
            self.memory_request,
            self.config.schema.clone(),
            StatsState::NotMaterialized,
        );

        // Set scheduling strategy based on whether we have a valid worker ID
        let scheduling_strategy = SchedulingStrategy::WorkerAffinity {
            worker_id,
            soft: false,
        };
        let psets = submittable_task.task().psets().clone();

        let task = submittable_task.with_new_task(SwordfishTask::new(
            task_context,
            actor_pool_project_plan,
            self.config.execution_config.clone(),
            psets,
            scheduling_strategy,
            self.context.to_hashmap(),
        ));
        Ok(task)
    }

    fn multiline_display(&self) -> Vec<String> {
        use daft_dsl::functions::python::{get_concurrency, get_resource_request, get_udf_names};
        use itertools::Itertools;
        let mut res = vec![];
        res.push("ActorUDF:".to_string());
        res.push(format!(
            "Projection = [{}]",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "UDFs = [{}]",
            self.projection
                .iter()
                .flat_map(|expr| get_udf_names(expr.inner()))
                .join(", ")
        ));
        res.push(format!(
            "Concurrency = {}",
            get_concurrency(
                &self
                    .projection
                    .iter()
                    .map(|e| e.inner().clone())
                    .collect::<Vec<_>>()
            )
        ));
        if let Some(resource_request) = get_resource_request(
            &self
                .projection
                .iter()
                .map(|e| e.inner().clone())
                .collect::<Vec<_>>(),
        ) {
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

    fn start(self: Arc<Self>, stage_context: &mut StageExecutionContext) -> RunningPipelineNode {
        let input_node = self.child.clone().start(stage_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop =
            self.execution_loop_fused(input_node, result_tx, stage_context.task_id_counter());
        stage_context.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
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
