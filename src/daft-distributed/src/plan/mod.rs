use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_logical_plan::{LogicalPlanBuilder, LogicalPlanRef};
use futures::StreamExt;

use crate::{
    scheduling::{
        scheduler::{SchedulerActor, SchedulerHandle},
        task::SwordfishTask,
        worker::{Worker, WorkerManager},
    },
    stage::StagePlan,
    utils::{
        channel::{create_channel, Receiver, ReceiverStream, Sender},
        joinset::{create_join_set, JoinSet},
        runtime::get_or_init_runtime,
        stream::JoinableForwardingStream,
    },
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
        stage_plan: StagePlan,
        psets: HashMap<String, Vec<PartitionRef>>,
        config: Arc<DaftExecutionConfig>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        sender: Sender<PartitionRef>,
    ) -> DaftResult<()> {
        if stage_plan.num_stages() != 1 {
            return Err(DaftError::ValueError(format!(
                "Cannot run multiple stages on flotilla yet. Got {} stages",
                stage_plan.num_stages()
            )));
        }

        let stage = stage_plan.get_root_stage();
        let running_stage = stage.run_stage(psets, config, scheduler_handle.clone())?;
        let mut materialized_stage = running_stage.materialize(scheduler_handle);
        while let Some(result) = materialized_stage.next().await {
            if sender.send(result?).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    pub fn run_plan<W: Worker>(
        &self,
        psets: HashMap<String, Vec<PartitionRef>>,
        worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    ) -> DaftResult<PlanResult> {
        let stage_plan = StagePlan::from_logical_plan(self.logical_plan.clone())?;
        let config = self.config.clone();

        let runtime = get_or_init_runtime();
        let mut joinset = create_join_set();

        let scheduler_actor = SchedulerActor::default_scheduler(worker_manager);
        let scheduler_handle = SchedulerActor::spawn_scheduler_actor(scheduler_actor, &mut joinset);

        let (result_sender, result_receiver) = create_channel(1);
        joinset.spawn_on(
            async move {
                Self::execute_stages(stage_plan, psets, config, scheduler_handle, result_sender)
                    .await
            },
            runtime.runtime.handle(),
        );
        Ok(PlanResult::new(joinset, result_receiver))
    }

    #[allow(dead_code)]
    pub fn execution_config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
    }
}

pub(crate) type PlanResultStream = JoinableForwardingStream<ReceiverStream<PartitionRef>>;

pub(crate) struct PlanResult {
    joinset: JoinSet<DaftResult<()>>,
    rx: Receiver<PartitionRef>,
}

impl PlanResult {
    fn new(joinset: JoinSet<DaftResult<()>>, rx: Receiver<PartitionRef>) -> Self {
        Self { joinset, rx }
    }

    pub fn into_stream(self) -> PlanResultStream {
        JoinableForwardingStream::new(ReceiverStream::new(self.rx), self.joinset)
    }
}
