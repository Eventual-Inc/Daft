use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use futures::StreamExt;

use super::{DistributedPhysicalPlan, PlanResult};
use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::{
        scheduler::{spawn_default_scheduler_actor, SchedulerHandle},
        task::SwordfishTask,
        worker::{Worker, WorkerManager},
    },
    stage::StagePlan,
    utils::{
        channel::{create_channel, Sender},
        joinset::create_join_set,
        runtime::get_or_init_runtime,
    },
};

pub(crate) struct PlanRunner<W: Worker<Task = SwordfishTask>> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
}

impl<W: Worker<Task = SwordfishTask>> PlanRunner<W> {
    pub fn new(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self { worker_manager }
    }

    async fn execute_stages(
        stage_plan: StagePlan,
        psets: HashMap<String, Vec<PartitionRef>>,
        config: Arc<DaftExecutionConfig>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        sender: Sender<MaterializedOutput>,
    ) -> DaftResult<()> {
        if stage_plan.num_stages() != 1 {
            return Err(DaftError::ValueError(format!(
                "Cannot run multiple stages on flotilla yet. Got {} stages",
                stage_plan.num_stages()
            )));
        }

        let stage = stage_plan.get_root_stage();
        let running_stage = stage.run_stage(psets, config, scheduler_handle.clone())?;
        let mut materialized_result_stream = running_stage.materialize(scheduler_handle);
        while let Some(result) = materialized_result_stream.next().await {
            if sender.send(result?).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    pub fn run_plan(
        &self,
        plan: &DistributedPhysicalPlan,
        psets: HashMap<String, Vec<PartitionRef>>,
    ) -> DaftResult<PlanResult> {
        let config = plan.execution_config().clone();
        let stage_plan = plan.stage_plan().clone();

        let runtime = get_or_init_runtime();
        let mut joinset = create_join_set();

        let scheduler_handle =
            spawn_default_scheduler_actor(self.worker_manager.clone(), &mut joinset);

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
}
