use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::stats::StatsState;

use super::{DistributedPipelineNode, PipelineOutput, RunningPipelineNode};
use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask},
    },
    stage::{StageContext, StageID},
    utils::channel::{create_channel, Sender},
};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ScanSourceNode {
    plan_id: PlanID,
    stage_id: StageID,
    node_id: NodeID,
    config: Arc<DaftExecutionConfig>,
    plan: LocalPhysicalPlanRef,
    pushdowns: Pushdowns,
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
}

impl ScanSourceNode {
    #[allow(dead_code)]
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
            plan_id,
            stage_id,
            node_id,
            config,
            plan,
            pushdowns,
            scan_tasks,
        }
    }

    async fn execution_loop(
        self,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
    ) -> DaftResult<()> {
        if self.scan_tasks.is_empty() {
            let empty_scan_task = self.make_empty_scan_task()?;
            let _ = result_tx
                .send(PipelineOutput::Task(SubmittableTask::new(empty_scan_task)))
                .await;
            return Ok(());
        }

        for scan_task in self.scan_tasks.iter() {
            let task = self.make_source_tasks(scan_task.clone())?;
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

    fn make_source_tasks(&self, scan_task: ScanTaskLikeRef) -> DaftResult<SwordfishTask> {
        let transformed_plan = self
            .plan
            .clone()
            .transform_up(|p| match p.as_ref() {
                LocalPhysicalPlan::PlaceholderScan(placeholder) => {
                    let physical_scan = LocalPhysicalPlan::physical_scan(
                        vec![scan_task.clone()].into(),
                        self.pushdowns.clone(),
                        placeholder.schema.clone(),
                        StatsState::NotMaterialized,
                    );
                    Ok(Transformed::yes(physical_scan))
                }
                _ => Ok(Transformed::no(p)),
            })?
            .data;

        let psets = HashMap::new();
        let context = HashMap::from([
            ("plan_id".to_string(), self.plan_id.to_string()),
            ("stage_id".to_string(), format!("{}", self.stage_id)),
            ("node_id".to_string(), format!("{}", self.node_id)),
            ("node_name".to_string(), self.name().to_string()),
        ]);
        let task = SwordfishTask::new(
            transformed_plan,
            self.config.clone(),
            psets,
            SchedulingStrategy::Spread,
            context,
            self.node_id,
        );
        Ok(task)
    }
    fn make_empty_scan_task(&self) -> DaftResult<SwordfishTask> {
        let transformed_plan = self
            .plan
            .clone()
            .transform_up(|p| match p.as_ref() {
                LocalPhysicalPlan::PlaceholderScan(placeholder) => {
                    let empty_scan = LocalPhysicalPlan::empty_scan(placeholder.schema.clone());
                    Ok(Transformed::yes(empty_scan))
                }
                _ => Ok(Transformed::no(p)),
            })?
            .data;
        let context = HashMap::from([
            ("plan_id".to_string(), self.plan_id.to_string()),
            ("stage_id".to_string(), format!("{}", self.stage_id)),
            ("node_id".to_string(), format!("{}", self.node_id)),
            ("node_name".to_string(), self.name().to_string()),
        ]);

        let psets = HashMap::new();
        let task = SwordfishTask::new(
            transformed_plan,
            self.config.clone(),
            psets,
            SchedulingStrategy::Spread,
            context,
            self.node_id,
        );
        Ok(task)
    }
}

impl DistributedPipelineNode for ScanSourceNode {
    fn name(&self) -> &'static str {
        "ScanSource"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![]
    }

    fn start(&self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.clone().execution_loop(result_tx);
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn plan_id(&self) -> &PlanID {
        &self.plan_id
    }

    fn stage_id(&self) -> &StageID {
        &self.stage_id
    }

    fn node_id(&self) -> &NodeID {
        &self.node_id
    }
}
