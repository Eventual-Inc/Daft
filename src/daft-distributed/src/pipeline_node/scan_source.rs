use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use daft_local_plan::LocalPhysicalPlanRef;
use daft_scan::{ScanTask, ScanTaskRef};

use super::{DistributedPipelineNode, PipelineOutput, RunningPipelineNode};
use crate::{
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask},
    },
    stage::StageContext,
    utils::channel::{create_channel, Sender},
};

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct ScanSourceNode {
    node_id: usize,
    config: Arc<DaftExecutionConfig>,
    plan: LocalPhysicalPlanRef,
    pushdowns: Pushdowns,
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
}

impl ScanSourceNode {
    #[allow(dead_code)]
    pub fn new(
        node_id: usize,
        config: Arc<DaftExecutionConfig>,
        plan: LocalPhysicalPlanRef,
        pushdowns: Pushdowns,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    ) -> Self {
        Self {
            node_id,
            config,
            plan,
            pushdowns,
            scan_tasks,
        }
    }

    async fn execution_loop(
        plan: LocalPhysicalPlanRef,
        config: Arc<DaftExecutionConfig>,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        node_id: usize,
    ) -> DaftResult<()> {
        for (scan_task_idx, scan_task) in scan_tasks.iter().enumerate() {
            let scan_task = scan_task
                .clone()
                .as_any_arc()
                .downcast::<ScanTask>()
                .unwrap();
            let task = make_source_tasks(
                plan.clone(),
                scan_task,
                scan_task_idx,
                config.clone(),
                node_id,
            )?;
            if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                break;
            }
        }

        Ok(())
    }
}

impl DistributedPipelineNode for ScanSourceNode {
    fn name(&self) -> &'static str {
        "ScanSource"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![]
    }

    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = Self::execution_loop(
            self.plan.clone(),
            self.config.clone(),
            self.scan_tasks.clone(),
            result_tx,
            self.node_id,
        );
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }
}

fn make_source_tasks(
    plan: LocalPhysicalPlanRef,
    scan_task: ScanTaskRef,
    input_id: usize,
    config: Arc<DaftExecutionConfig>,
    node_id: usize,
) -> DaftResult<SubmittableTask<SwordfishTask>> {
    let task = SwordfishTask::new(
        node_id.to_string().into(),
        plan,
        input_id,
        scan_task.into(),
        config,
        node_id as u32,
        SchedulingStrategy::Spread,
    );
    Ok(SubmittableTask::new(task))
}
