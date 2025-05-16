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
use daft_logical_plan::LogicalPlanBuilder;
use futures::{FutureExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    scheduling::worker::{Worker, WorkerManager},
    stage::{StagePlan, StagePlanRef},
    utils::{
        channel::{create_channel, Receiver, Sender},
        runtime::get_or_init_runtime,
    },
};

#[derive(Serialize, Deserialize)]
pub struct DistributedPhysicalPlan {
    stage_plan: StagePlanRef,
    config: Arc<DaftExecutionConfig>,
}

impl DistributedPhysicalPlan {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let plan = builder.build();

        Ok(Self {
            stage_plan: Arc::new(StagePlan::from_logical_plan(plan)?),
            config,
        })
    }

    async fn execute_stages<W: Worker>(
        stage_plan: StagePlanRef,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
        worker_manager: Arc<dyn WorkerManager<Worker = W>>,
        sender: Sender<PartitionRef>,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<()> {
        if stage_plan.num_stages() != 1 {
            return Err(DaftError::ValueError(format!(
                "Cannot run multiple stages on flotilla yet. Got {} stages",
                stage_plan.num_stages()
            )));
        }

        let stage = stage_plan.get_root_stage();
        let mut running_stage = stage.run_stage(psets, worker_manager, config)?;
        while let Some(materialized_output) = running_stage.next().await {
            let materialized_output = materialized_output?;
            let (partition, _) = materialized_output.into_inner();
            if sender.send(partition).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    pub fn run_plan<W: Worker>(
        &self,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
        worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    ) -> DaftResult<PlanResult> {
        let (result_sender, result_receiver) = create_channel(1);
        let runtime = get_or_init_runtime();
        let handle = runtime.spawn(Self::execute_stages(
            self.stage_plan.clone(),
            psets,
            worker_manager,
            result_sender,
            self.config.clone(),
        ));
        Ok(PlanResult::new(handle, result_receiver))
    }

    pub fn execution_config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
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
