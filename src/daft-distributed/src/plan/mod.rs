use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use common_runtime::RuntimeTask;
use daft_logical_plan::{LogicalPlan, LogicalPlanBuilder, LogicalPlanRef};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};

use crate::{
    channel::{create_channel, Receiver, Sender},
    runtime::get_or_init_runtime,
    scheduling::worker::WorkerManagerFactory,
    stage::split_at_stage_boundary,
};

pub struct DistributedPhysicalPlan {
    remaining_logical_plan: Option<LogicalPlanRef>,
    config: Arc<DaftExecutionConfig>,
}

impl DistributedPhysicalPlan {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let plan = builder.build();
        if !can_translate_logical_plan(&plan) {
            return Err(DaftError::InternalError(
                "Cannot run this physical plan on distributed swordfish yet".to_string(),
            ));
        }

        Ok(Self {
            remaining_logical_plan: Some(plan),
            config,
        })
    }

    async fn run_plan_loop(
        logical_plan: LogicalPlanRef,
        config: Arc<DaftExecutionConfig>,
        worker_manager_factory: Box<dyn WorkerManagerFactory>,
        mut psets: HashMap<String, Vec<PartitionRef>>,
        result_sender: Sender<PartitionRef>,
    ) -> DaftResult<()> {
        let mut remaining_logical_plan = Some(logical_plan);
        while let Some(plan) = remaining_logical_plan.take() {
            let (stage, remaining_plan) = split_at_stage_boundary(&plan, &config)?;
            match remaining_plan {
                Some(remaining_plan) => {
                    let running_stage = stage
                        .run_stage(std::mem::take(&mut psets), worker_manager_factory.as_ref())?;
                    let results = running_stage.try_collect::<Vec<_>>().await?;
                    let updated_plan = Self::update_plan(remaining_plan, results)?;
                    remaining_logical_plan = Some(updated_plan);
                }
                None => {
                    let mut running_stage = stage
                        .run_stage(std::mem::take(&mut psets), worker_manager_factory.as_ref())?;
                    while let Some(result) = running_stage.next().await {
                        if result_sender.send(result?).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn update_plan(
        _plan: LogicalPlanRef,
        _results: Vec<PartitionRef>,
    ) -> DaftResult<LogicalPlanRef> {
        // Update the logical plan with the results of the previous stage.
        // This is where the AQE magic happens.
        todo!("Implement plan updating and AQE");
    }

    pub fn run_plan(
        &self,
        psets: HashMap<String, Vec<PartitionRef>>,
        worker_manager_factory: Box<dyn WorkerManagerFactory>,
    ) -> PlanResult {
        let (result_sender, result_receiver) = create_channel(1);
        let runtime = get_or_init_runtime();
        let handle = runtime.spawn(Self::run_plan_loop(
            self.remaining_logical_plan
                .as_ref()
                .expect("Expected remaining logical plan")
                .clone(),
            self.config.clone(),
            worker_manager_factory,
            psets,
            result_sender,
        ));
        PlanResult::new(handle, result_receiver)
    }

    pub fn execution_config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
    }
}

fn can_translate_logical_plan(plan: &LogicalPlanRef) -> bool {
    match plan.as_ref() {
        LogicalPlan::Source(_) => true,
        LogicalPlan::Project(project) => can_translate_logical_plan(&project.input),
        LogicalPlan::Filter(filter) => can_translate_logical_plan(&filter.input),
        LogicalPlan::Sink(sink) => can_translate_logical_plan(&sink.input),
        LogicalPlan::Sample(sample) => can_translate_logical_plan(&sample.input),
        LogicalPlan::Explode(explode) => can_translate_logical_plan(&explode.input),
        LogicalPlan::Unpivot(unpivot) => can_translate_logical_plan(&unpivot.input),
        LogicalPlan::Limit(_) => false,
        LogicalPlan::ActorPoolProject(_) => false,
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

// This is the output of a plan, a receiver to receive the results of the plan.
// And the join handle to the task that runs the plan.
pub struct PlanResult {
    task: Option<RuntimeTask<DaftResult<()>>>,
    rx: Receiver<PartitionRef>,
}

impl PlanResult {
    fn new(task: RuntimeTask<DaftResult<()>>, rx: Receiver<PartitionRef>) -> Self {
        Self {
            task: Some(task),
            rx,
        }
    }
}

impl Stream for PlanResult {
    type Item = DaftResult<PartitionRef>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.task.is_none() {
            return Poll::Ready(None);
        }

        match self.rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(result)) => Poll::Ready(Some(Ok(result))),
            Poll::Ready(None) => {
                if let Some(mut handle) = self.task.take() {
                    let result = handle.poll_unpin(cx);
                    match result {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(Ok(Ok(()))) => Poll::Ready(None),
                        Poll::Ready(Ok(Err(e))) | Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                    }
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}
