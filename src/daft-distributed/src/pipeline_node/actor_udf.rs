use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_dsl::{
    expr::bound_expr::BoundExpr,
    functions::python::{get_concurrency, get_resource_request, try_get_batch_size_from_udf},
    pyobj_serde::PyObjectWrapper,
    python::PyExpr,
    ExprRef,
};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;
use pyo3::{types::PyAnyMethods, PyObject, Python};

use super::{
    DisplayLevel, DistributedPipelineNode, MaterializedOutput, NodeID, PipelineOutput,
    RunningPipelineNode, TreeDisplay,
};
use crate::{
    plan::PlanID,
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask},
        worker::WorkerId,
    },
    stage::{StageContext, StageID},
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

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct ActorUDF {
    plan_id: PlanID,
    stage_id: StageID,
    node_id: NodeID,
    config: Arc<DaftExecutionConfig>,
    child: Arc<dyn DistributedPipelineNode>,
    schema: SchemaRef,
    projection: Vec<BoundExpr>,
    batch_size: Option<usize>,
    memory_request: u64,
}

impl ActorUDF {
    #[allow(dead_code)]
    pub fn new(
        plan_id: PlanID,
        stage_id: StageID,
        node_id: NodeID,
        config: Arc<DaftExecutionConfig>,
        projection: Vec<ExprRef>,
        schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> DaftResult<Self> {
        let batch_size = try_get_batch_size_from_udf(&projection)?;
        let memory_request = get_resource_request(&projection)
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);
        let projection = BoundExpr::bind_all(&projection, &schema)?;
        Ok(Self {
            plan_id,
            stage_id,
            node_id,
            config,
            child,
            schema,
            projection,
            batch_size,
            memory_request,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn execution_loop_fused(
        self,
        input: RunningPipelineNode,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
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
                        vec![materialized_output],
                        worker_id,
                        actors,
                    )?;
                    let (submittable_task, notify_token) = task.with_notify_token();
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

                    let modified_task = self.append_actor_udf_to_task(worker_id, task, actors)?;
                    let (submittable_task, notify_token) = modified_task.with_notify_token();
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

    #[allow(clippy::too_many_arguments)]
    fn make_actor_udf_task_for_materialized_outputs(
        &self,
        materialized_outputs: Vec<MaterializedOutput>,
        worker_id: WorkerId,
        actors: Vec<PyObjectWrapper>,
    ) -> DaftResult<SubmittableTask<SwordfishTask>> {
        // Extract all partitions from materialized outputs
        let mut partitions = Vec::new();
        for materialized_output in materialized_outputs {
            let (partition, _) = materialized_output.into_inner();
            partitions.push(partition);
        }

        let in_memory_info = InMemoryInfo::new(
            self.schema.clone(),
            self.node_id.to_string(),
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
            self.schema.clone(),
            StatsState::NotMaterialized,
        );
        let context = HashMap::from([
            ("plan_id".to_string(), self.plan_id.to_string()),
            ("stage_id".to_string(), format!("{}", self.stage_id)),
            ("node_id".to_string(), format!("{}", self.node_id)),
            ("node_name".to_string(), self.name().to_string()),
        ]);
        let task = SubmittableTask::new(SwordfishTask::new(
            actor_pool_project_plan,
            self.config.clone(),
            HashMap::from([(self.node_id.to_string(), partitions)]),
            SchedulingStrategy::WorkerAffinity {
                worker_id,
                soft: false,
            },
            context,
            self.node_id,
        ));

        Ok(task)
    }

    #[allow(clippy::too_many_arguments)]
    fn append_actor_udf_to_task(
        &self,
        worker_id: WorkerId,
        submittable_task: SubmittableTask<SwordfishTask>,
        actors: Vec<PyObjectWrapper>,
    ) -> DaftResult<SubmittableTask<SwordfishTask>> {
        let task_plan = submittable_task.task().plan();
        let actor_pool_project_plan = LocalPhysicalPlan::distributed_actor_pool_project(
            task_plan,
            actors,
            self.batch_size,
            self.memory_request,
            self.schema.clone(),
            StatsState::NotMaterialized,
        );

        // Set scheduling strategy based on whether we have a valid worker ID
        let scheduling_strategy = SchedulingStrategy::WorkerAffinity {
            worker_id,
            soft: false,
        };
        let psets = submittable_task.task().psets().clone();

        let context = HashMap::from([
            ("plan_id".to_string(), self.plan_id.to_string()),
            ("stage_id".to_string(), format!("{}", self.stage_id)),
            ("node_id".to_string(), format!("{}", self.node_id)),
            ("node_name".to_string(), self.name().to_string()),
        ]);
        let task = submittable_task.with_new_task(SwordfishTask::new(
            actor_pool_project_plan,
            self.config.clone(),
            psets,
            scheduling_strategy,
            context,
            self.node_id,
        ));
        Ok(task)
    }
}

impl DistributedPipelineNode for ActorUDF {
    fn name(&self) -> &'static str {
        "ActorUDF"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![self.child.as_ref()]
    }

    fn start(&self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let input_node = self.child.start(stage_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.clone().execution_loop_fused(input_node, result_tx);
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn plan_id(&self) -> &PlanID {
        &self.plan_id
    }
    fn stage_id(&self) -> &StageID {
        &self.stage_id
    }
    fn node_id(&self) -> &NodeID {
        &self.node_id
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

impl TreeDisplay for ActorUDF {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        writeln!(display, "{}", self.name()).unwrap();
        writeln!(display, "Node ID: {}", self.node_id).unwrap();
        let plan = self
            .make_actor_udf_task_for_materialized_outputs(vec![], WorkerId::default(), vec![])
            .unwrap();
        writeln!(display, "{}", plan.task().plan().single_line_display()).unwrap();
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.name().to_string()
    }
}
