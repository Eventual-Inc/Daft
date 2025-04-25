use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_logical_plan::{LogicalPlan, LogicalPlanBuilder, LogicalPlanRef};

use crate::{
    channel::{create_channel, Receiver, Sender},
    runtime::{create_join_set, get_or_init_runtime, JoinHandle},
    scheduling::{dispatcher::TaskDispatcher, worker::WorkerManager},
    stage::{split_at_stage_boundary, Stage},
};

pub struct DistributedPhysicalPlan {
    remaining_logical_plan: Option<LogicalPlanRef>,
    config: Arc<DaftExecutionConfig>,
}

impl DistributedPhysicalPlan {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        config: &Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let plan = builder.build();
        if !can_translate_logical_plan(&plan) {
            return Err(DaftError::InternalError(
                "Cannot run this physical plan on distributed swordfish yet".to_string(),
            ));
        }

        Ok(Self {
            remaining_logical_plan: Some(plan),
            config: config.clone(),
        })
    }

    pub async fn plan_loop(
        mut remaining_logical_plan: Option<LogicalPlanRef>,
        config: Arc<DaftExecutionConfig>,
        worker_manager_creator: Arc<dyn Fn() -> Box<dyn WorkerManager> + Send + Sync>,
        mut psets: HashMap<String, Vec<PartitionRef>>,
        result_sender: Sender<PartitionRef>,
    ) -> DaftResult<()> {
        while let Some(remaining_plan) = remaining_logical_plan.take() {
            let worker_manager = worker_manager_creator();
            let task_dispatcher = TaskDispatcher::new(worker_manager);
            let mut joinset = create_join_set();
            let task_dispatcher_handle =
                TaskDispatcher::spawn_task_dispatcher(task_dispatcher, &mut joinset)?;

            let (next_stage, remaining_plan) = split_at_stage_boundary(&remaining_plan, &config)?;

            match (next_stage, remaining_plan) {
                (Stage::Collect(collect_stage), None) => {
                    let mut stage_result_receiver = collect_stage.spawn_stage_programs(
                        std::mem::take(&mut psets),
                        task_dispatcher_handle,
                        &mut joinset,
                    )?;
                    while let Some(result) = stage_result_receiver.recv().await {
                        if result_sender.send(result).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                (Stage::ShuffleMap(shuffle_map_stage), Some(remaining_plan)) => {
                    let mut stage_result_receiver = shuffle_map_stage.spawn_stage_programs(
                        std::mem::take(&mut psets),
                        task_dispatcher_handle,
                        &mut joinset,
                    )?;
                    let mut results = Vec::new();
                    while let Some(result) = stage_result_receiver.recv().await {
                        results.push(result);
                    }
                    remaining_logical_plan = Some(Self::update(remaining_plan, results)?);
                }
                (_, _) => panic!("We only support collect stages for now"),
            }
            while let Some(result) = joinset.join_next().await {
                result.map_err(|e| DaftError::InternalError(e.to_string()))??;
            }
        }
        Ok(())
    }

    pub fn update(
        _plan: LogicalPlanRef,
        _results: Vec<PartitionRef>,
    ) -> DaftResult<LogicalPlanRef> {
        todo!()
    }

    pub fn run_plan(
        &mut self,
        psets: HashMap<String, Vec<PartitionRef>>,
        worker_manager_creator: Arc<dyn Fn() -> Box<dyn WorkerManager> + Send + Sync>,
    ) -> PlanResultProducer {
        let (result_sender, result_receiver) = create_channel(1);
        let runtime = get_or_init_runtime();
        let handle = runtime.spawn(Self::plan_loop(
            self.remaining_logical_plan.take(),
            self.config.clone(),
            worker_manager_creator,
            psets,
            result_sender,
        ));
        PlanResultProducer::new(handle, result_receiver)
    }
}

fn can_translate_logical_plan(plan: &LogicalPlanRef) -> bool {
    match plan.as_ref() {
        LogicalPlan::Source(_) => true,
        LogicalPlan::Project(project) => can_translate_logical_plan(&project.input),
        LogicalPlan::ActorPoolProject(actor_pool_project) => {
            can_translate_logical_plan(&actor_pool_project.input)
        }
        LogicalPlan::Filter(filter) => can_translate_logical_plan(&filter.input),
        LogicalPlan::Sink(sink) => can_translate_logical_plan(&sink.input),
        LogicalPlan::Sample(sample) => can_translate_logical_plan(&sample.input),
        LogicalPlan::Explode(explode) => can_translate_logical_plan(&explode.input),
        LogicalPlan::Unpivot(unpivot) => can_translate_logical_plan(&unpivot.input),
        LogicalPlan::Limit(limit) => can_translate_logical_plan(&limit.input),
        LogicalPlan::Pivot(_) => false,
        LogicalPlan::Sort(_) => false,
        LogicalPlan::Distinct(_) => false,
        LogicalPlan::Aggregate(_) => false,
        LogicalPlan::Window(_) => false,
        LogicalPlan::Concat(_) => false,
        LogicalPlan::Intersect(_) => false,
        LogicalPlan::Union(_) => false,
        LogicalPlan::Join(_) => true,
        LogicalPlan::Repartition(_) => false,
        LogicalPlan::SubqueryAlias(_) => false,
        LogicalPlan::MonotonicallyIncreasingId(_) => false,
    }
}

pub struct PlanResultProducer {
    handle: Option<JoinHandle<DaftResult<()>>>,
    rx: Receiver<PartitionRef>,
}

impl PlanResultProducer {
    pub fn new(handle: JoinHandle<DaftResult<()>>, rx: Receiver<PartitionRef>) -> Self {
        Self {
            handle: Some(handle),
            rx,
        }
    }

    pub async fn get_next(&mut self) -> Option<DaftResult<PartitionRef>> {
        self.handle.as_ref()?;
        match self.rx.recv().await {
            Some(result) => Some(Ok(result)),
            None => {
                if let Some(handle) = self.handle.take() {
                    let res = handle
                        .await
                        .map_err(|e| DaftError::InternalError(e.to_string()));
                    match res {
                        Ok(Ok(())) => None,
                        Ok(Err(e)) => Some(Err(e)),
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    None
                }
            }
        }
    }
}
