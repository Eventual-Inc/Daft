use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_logical_plan::{LogicalPlanBuilder, LogicalPlanRef};
use futures::Stream;

use crate::{
    channel::{create_channel, Receiver},
    runtime::{get_or_init_runtime, JoinHandle},
    scheduling::worker::WorkerManagerFactory,
    stage::StagePlan,
};

pub struct DistributedPhysicalPlan {
    #[allow(dead_code)]
    logical_plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
}

impl DistributedPhysicalPlan {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let plan = builder.build();

        Ok(Self {
            logical_plan: plan,
            config,
        })
    }

    async fn execute_stages(
        _stage_plan: StagePlan,
        _psets: HashMap<String, Vec<PartitionRef>>,
        _worker_manager_factory: Box<dyn WorkerManagerFactory>,
    ) -> DaftResult<()> {
        todo!("FLOTILLA_MS1: Implement execute stages");
    }

    pub fn run_plan(
        &self,
        psets: HashMap<String, Vec<PartitionRef>>,
        worker_manager_factory: Box<dyn WorkerManagerFactory>,
    ) -> DaftResult<PlanResult> {
        let (_result_sender, result_receiver) = create_channel(1);
        let runtime = get_or_init_runtime();
        let stage_plan = StagePlan::from_logical_plan(self.logical_plan.clone())?;
        let handle = runtime.spawn(async move {
            Self::execute_stages(stage_plan, psets, worker_manager_factory).await
        });
        Ok(PlanResult::new(handle, result_receiver))
    }

    pub fn execution_config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
    }
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
        todo!("FLOTILLA_MS1: Implement stream for plan result");
    }
}
