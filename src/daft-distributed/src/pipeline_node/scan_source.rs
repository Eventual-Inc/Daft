use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use daft_local_plan::LocalPhysicalPlanRef;

use super::{
    DistributedPipelineNode, DistributedPipelineNodeContext, PipelineOutput, RunningPipelineNode,
};
use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, SwordfishTaskInput},
    },
    stage::{StageContext, StageID},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct ScanSourceNode {
    context: DistributedPipelineNodeContext,
    config: Arc<DaftExecutionConfig>,
    plan: LocalPhysicalPlanRef,
    pushdowns: Pushdowns,
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
}

impl ScanSourceNode {
    const NODE_NAME: &'static str = "ScanSource";

    pub fn new(
        plan_id: PlanID,
        stage_id: StageID,
        node_id: NodeID,
        config: Arc<DaftExecutionConfig>,
        plan: LocalPhysicalPlanRef,
        pushdowns: Pushdowns,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    ) -> Self {
        Self {
            context: DistributedPipelineNodeContext::new(
                plan_id,
                stage_id,
                node_id,
                Self::NODE_NAME,
            ),
            config,
            plan,
            pushdowns,
            scan_tasks,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
    ) -> DaftResult<()> {
        if self.scan_tasks.is_empty() {
            let empty_scan_task = self.make_empty_scan_task()?;
            let _ = result_tx
                .send(PipelineOutput::Task(SubmittableTask::new(empty_scan_task)))
                .await;
            return Ok(());
        }

        let max_sources_per_scan_task = self.config.max_sources_per_scan_task;
        for scan_tasks in self.scan_tasks.chunks(max_sources_per_scan_task) {
            let task = self.make_source_tasks(scan_tasks.to_vec().into())?;
            if result_tx
                .send(PipelineOutput::Task(SubmittableTask::new(task)))
                .await
                .is_err()
            {
                break;
            }
        }

        Ok(())
    }

    fn make_source_tasks(
        &self,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    ) -> DaftResult<SwordfishTask> {
        let task = SwordfishTask::new(
            self.plan.clone(),
            self.config.clone(),
            SwordfishTaskInput::ScanTasks(scan_tasks, self.pushdowns.clone()),
            SchedulingStrategy::Spread,
            self.context.clone().into(),
        );
        Ok(task)
    }

    fn make_empty_scan_task(&self) -> DaftResult<SwordfishTask> {
        let task = SwordfishTask::new(
            self.plan.clone(),
            self.config.clone(),
            SwordfishTaskInput::ScanTasks(Arc::new(vec![]), self.pushdowns.clone()),
            SchedulingStrategy::Spread,
            self.context.clone().into(),
        );
        Ok(task)
    }
}

impl DistributedPipelineNode for ScanSourceNode {
    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageContext) -> RunningPipelineNode {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(result_tx);
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn context(&self) -> DistributedPipelineNodeContext {
        self.context.clone()
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

impl TreeDisplay for ScanSourceNode {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.name()).unwrap();
        writeln!(display, "Node ID: {}", self.node_id()).unwrap();

        let plan = self
            .make_source_tasks(self.scan_tasks.clone())
            .unwrap()
            .plan
            .clone();
        writeln!(display, "Local Plan: {}", plan.single_line_display()).unwrap();
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }

    fn get_name(&self) -> String {
        self.name().to_string()
    }
}
