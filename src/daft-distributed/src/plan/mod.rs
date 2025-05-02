use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_logical_plan::{LogicalPlanBuilder, LogicalPlanRef};
use futures::{Stream, StreamExt};

use crate::{
    channel::{create_channel, Receiver, Sender},
    runtime::{get_or_init_runtime, JoinHandle},
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
        psets: HashMap<String, Vec<PartitionRef>>,
        result_sender: Sender<PartitionRef>,
    ) -> DaftResult<()> {
        let (stage, _remaining_plan) = split_at_stage_boundary(&logical_plan, &config)?;
        let mut running_stage = stage.run_stage(psets, worker_manager_factory)?;
        while let Some(result) = running_stage.next().await {
            if result_sender.send(result?).await.is_err() {
                break;
            }
        }
        todo!("Implement stage running loop");
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

fn can_translate_logical_plan(_plan: &LogicalPlanRef) -> bool {
    todo!("Implement logical plan translation check");
}

// This is the output of a plan, a receiver to receive the results of the plan.
// And the join handle to the task that runs the plan.
pub struct PlanResult {
    _handle: Option<JoinHandle<DaftResult<()>>>,
    _rx: Receiver<PartitionRef>,
}

impl PlanResult {
    fn new(handle: JoinHandle<DaftResult<()>>, rx: Receiver<PartitionRef>) -> Self {
        Self {
            _handle: Some(handle),
            _rx: rx,
        }
    }
}

impl Stream for PlanResult {
    type Item = DaftResult<PartitionRef>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!("Implement stream for plan result");
    }
}
