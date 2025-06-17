use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use futures::StreamExt;
use tracing::info_span;

use super::{DistributedPhysicalPlan, PlanResult};
use crate::{
    pipeline_node::{logical_plan_to_pipeline_node, MaterializedOutput},
    scheduling::{
        scheduler::{spawn_default_scheduler_actor, SchedulerHandle},
        task::SwordfishTask,
        worker::{Worker, WorkerManager},
    },
    stage::{RunningStage, Stage, StageContext, StagePlan, StageType},
    utils::{
        channel::{create_channel, Sender},
        joinset::create_join_set,
        runtime::get_or_init_runtime,
    },
    HooksManager, PlanSpan, StageSpan,
};
pub type PlanID = Arc<str>;

#[derive(Clone)]
pub(crate) struct PlanRunner<W: Worker<Task = SwordfishTask>> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    plan_id: PlanID,
    hooks_manager: HooksManager,
}

impl<W: Worker<Task = SwordfishTask>> PlanRunner<W> {
    pub fn new(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        let plan_id = uuid::Uuid::new_v4().to_string();
        Self {
            worker_manager,
            plan_id: Arc::from(plan_id),
            hooks_manager: HooksManager::new(),
        }
    }

    pub fn run_plan(
        self: &Arc<Self>,
        plan: &DistributedPhysicalPlan,
        psets: HashMap<String, Vec<PartitionRef>>,
    ) -> DaftResult<PlanResult> {
        let stage_plan = plan.stage_plan().clone();
        let plan_clone = plan.clone();

        let runtime = get_or_init_runtime();
        let (result_sender, result_receiver) = create_channel(1);

        let this = self.clone();

        let joinset = runtime.block_on_current_thread(async move {
            let mut joinset = create_join_set();
            let scheduler_handle =
                spawn_default_scheduler_actor(self.worker_manager.clone(), &mut joinset);

            joinset.spawn(async move {
                // Create a 'span' to track the creation and completion of a "plan"
                let _span = this.create_plan_span(&plan_clone);

                this.execute_stages(stage_plan, psets, scheduler_handle, result_sender)
                    .await
            });
            joinset
        });
        Ok(PlanResult::new(joinset, result_receiver))
    }

    async fn execute_stages(
        &self,
        stage_plan: StagePlan,
        psets: HashMap<String, Vec<PartitionRef>>,
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
        let running_stage = self.run_stage(
            stage,
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
        Ok(())
    }
    pub(crate) fn run_stage<'a>(
        &'a self,
        stage: &'a Stage,
        psets: HashMap<String, Vec<PartitionRef>>,
        config: Arc<DaftExecutionConfig>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<RunningStage<'a>> {
        let mut stage_context = self.new_stage_context(stage, scheduler_handle);
        match &stage.type_ {
            StageType::MapPipeline { plan } => {
                let pipeline_node = logical_plan_to_pipeline_node(
                    self.plan_id.clone(),
                    stage.id.clone(),
                    plan.clone(),
                    config,
                    Arc::new(psets),
                )?;
                let running_node = pipeline_node.start(&mut stage_context);
                Ok(RunningStage::new(running_node, stage_context))
            }
            _ => todo!("FLOTILLA_MS2: Implement run_stage for other stage types"),
        }
    }

    /// this function is used to create a span for the execution of a plan
    /// It will emit the `PlanEvent::PlanStarted` on start, and `PlanEvent::PlanCompleted` when the span goes out of scope.
    fn create_plan_span<'a>(&'a self, plan: &'a DistributedPhysicalPlan) -> PlanSpan<'a> {
        PlanSpan::new(plan, &self.plan_id, &self.hooks_manager)
    }

    fn new_stage_context<'a>(
        &'a self,
        stage: &'a Stage,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> StageContext<'a> {
        StageContext::new(
            scheduler_handle,
            StageSpan::new(stage, &self.plan_id, &self.hooks_manager),
        )
    }
}
