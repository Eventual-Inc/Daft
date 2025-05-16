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
use daft_logical_plan::{LogicalPlanBuilder, LogicalPlanRef};
use futures::{Stream, StreamExt};

use crate::{
    scheduling::worker::WorkerManagerFactory,
    stage::StagePlan,
    utils::{
        channel::{create_channel, Receiver, Sender},
        joinset::JoinSet,
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
        worker_manager_factory: Box<dyn WorkerManagerFactory>,
        _sender: Sender<PartitionRef>,
    ) -> DaftResult<()> {
        if stage_plan.num_stages() != 1 {
            return Err(DaftError::ValueError(format!(
                "Cannot run multiple stages on flotilla yet. Got {} stages",
                stage_plan.num_stages()
            )));
        }

        let worker_manager = worker_manager_factory.create_worker_manager()?;
        let stage = stage_plan.get_root_stage();
        let _running_stage = stage.run_stage(psets, config, worker_manager)?;
        // let materialized_stage = running_stage.materialize(task_dispatcher_handle);
        // while let Some(output) = materialized_stage.next().await {
        //     if sender.send(output).await.is_err() {
        //         break;
        //     }
        // }
        todo!("FLOTILLA_MS1: Implement execute_stages");
    }

    pub fn run_plan(
        &self,
        psets: HashMap<String, Vec<PartitionRef>>,
        worker_manager_factory: Box<dyn WorkerManagerFactory>,
    ) -> DaftResult<PlanResult> {
        let (result_sender, result_receiver) = create_channel(1);
        let runtime = get_or_init_runtime();
        let stage_plan = StagePlan::from_logical_plan(self.logical_plan.clone())?;
        let config = self.config.clone();
        let handle = runtime.spawn(async move {
            Self::execute_stages(
                stage_plan,
                psets,
                config,
                worker_manager_factory,
                result_sender,
            )
            .await
        });
        Ok(PlanResult::new(handle, result_receiver))
    }

    pub fn execution_config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
    }
}

pub(crate) struct PlanResult {
    stream: JoinableForwardingStream<tokio_stream::wrappers::ReceiverStream<PartitionRef>>,
}

impl PlanResult {
    fn new(task: RuntimeTask<DaftResult<()>>, rx: Receiver<PartitionRef>) -> Self {
        let inner_joinset = JoinSet::from(task.into_inner());
        let stream = JoinableForwardingStream::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
            inner_joinset,
        );
        Self { stream }
    }
}

impl Stream for PlanResult {
    type Item = DaftResult<PartitionRef>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}
