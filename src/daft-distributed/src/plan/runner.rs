use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use futures::StreamExt;

use super::{DistributedPhysicalPlan, PlanID, PlanResult};
use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::{
        scheduler::{spawn_default_scheduler_actor, SchedulerHandle},
        task::SwordfishTask,
        worker::{Worker, WorkerManager},
    },
    stage::StagePlan,
    statistics::{StatisticsEvent, StatisticsManagerRef},
    utils::{
        channel::{create_channel, Sender},
        joinset::create_join_set,
        runtime::get_or_init_runtime,
    },
};

#[derive(Clone)]
pub(crate) struct PlanRunner<W: Worker<Task = SwordfishTask>> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
}

impl<W: Worker<Task = SwordfishTask>> PlanRunner<W> {
    pub fn new(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self { worker_manager }
    }

    async fn execute_stages(
        &self,
        plan_id: PlanID,
        stage_plan: StagePlan,
        psets: HashMap<String, Vec<PartitionRef>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        sender: Sender<MaterializedOutput>,
        statistics_manager: StatisticsManagerRef,
    ) -> DaftResult<()> {
        if stage_plan.num_stages() != 1 {
            return Err(DaftError::ValueError(format!(
                "Cannot run multiple stages on flotilla yet. Got {} stages",
                stage_plan.num_stages()
            )));
        }

        let stage = stage_plan.get_root_stage();
        let running_stage = stage.run_stage(
            plan_id,
            psets,
            stage_plan.execution_config().clone(),
            scheduler_handle.clone(),
        )?;
        let mut materialized_result_stream = running_stage.materialize(scheduler_handle);
        while let Some(result) = materialized_result_stream.next().await {
            if sender.send(result?).await.is_err() {
                break;
            }
        }
        statistics_manager.handle_event(StatisticsEvent::PlanFinished { plan_id })?;
        Ok(())
    }

    pub fn run_plan(
        self: &Arc<Self>,
        plan: &DistributedPhysicalPlan,
        psets: HashMap<String, Vec<PartitionRef>>,
        statistics_manager: StatisticsManagerRef,
    ) -> DaftResult<PlanResult> {
        let plan_id = plan.id();
        let query_id = uuid::Uuid::new_v4().to_string();
        let stage_plan = plan.stage_plan().clone();

        let runtime = get_or_init_runtime();
        statistics_manager.handle_event(StatisticsEvent::PlanSubmitted {
            plan_id,
            query_id,
            logical_plan: plan.logical_plan().clone(),
        })?;

        let (result_sender, result_receiver) = create_channel(1);

        let this = self.clone();

        let joinset = runtime.block_on_current_thread(async move {
            let mut joinset = create_join_set();
            let scheduler_handle = spawn_default_scheduler_actor(
                self.worker_manager.clone(),
                &mut joinset,
                statistics_manager.clone(),
            );

            joinset.spawn(async move {
                this.execute_stages(
                    plan_id,
                    stage_plan,
                    psets,
                    scheduler_handle,
                    result_sender,
                    statistics_manager,
                )
                .await
            });
            joinset
        });
        Ok(PlanResult::new(joinset, result_receiver))
    }
}
