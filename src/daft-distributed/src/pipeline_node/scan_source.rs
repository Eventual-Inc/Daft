use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::stats::StatsState;

use super::{DistributedPipelineNode, PipelineOutput, RunningPipelineNode};
use crate::{
    scheduling::task::{SchedulingStrategy, SwordfishTask},
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
        pushdowns: Pushdowns,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
    ) -> DaftResult<()> {
        if scan_tasks.is_empty() {
            let empty_scan_task = make_empty_scan_task(&plan, config.clone())?;
            let _ = result_tx.send(PipelineOutput::Task(empty_scan_task)).await;
            return Ok(());
        }

        for scan_task in scan_tasks.iter() {
            let task = make_source_tasks(
                &plan,
                &pushdowns,
                vec![scan_task.clone()].into(),
                config.clone(),
            )?;
            if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                break;
            }
        }

        Ok(())
    }
}

impl TreeDisplay for ScanSourceNode {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.name()).unwrap();
        writeln!(display, "Node ID: {}", self.node_id).unwrap();

        let plan = make_source_tasks(
            &self.plan,
            &self.pushdowns,
            self.scan_tasks.clone(),
            self.config.clone(),
        )
        .unwrap()
        .plan();
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

impl DistributedPipelineNode for ScanSourceNode {
    fn name(&self) -> &'static str {
        "DistributedScan"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![]
    }

    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = Self::execution_loop(
            self.plan.clone(),
            self.config.clone(),
            self.pushdowns.clone(),
            self.scan_tasks.clone(),
            result_tx,
        );
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

fn make_source_tasks(
    plan: &LocalPhysicalPlanRef,
    pushdowns: &Pushdowns,
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    config: Arc<DaftExecutionConfig>,
) -> DaftResult<SwordfishTask> {
    let transformed_plan = plan
        .clone()
        .transform_up(|p| match p.as_ref() {
            LocalPhysicalPlan::PlaceholderScan(placeholder) => {
                let physical_scan = LocalPhysicalPlan::physical_scan(
                    scan_tasks.clone(),
                    pushdowns.clone(),
                    placeholder.schema.clone(),
                    StatsState::NotMaterialized,
                );
                Ok(Transformed::yes(physical_scan))
            }
            _ => Ok(Transformed::no(p)),
        })?
        .data;

    let psets = HashMap::new();
    let task = SwordfishTask::new(transformed_plan, config, psets, SchedulingStrategy::Spread);
    Ok(task)
}

fn make_empty_scan_task(
    plan: &LocalPhysicalPlanRef,
    config: Arc<DaftExecutionConfig>,
) -> DaftResult<SwordfishTask> {
    let transformed_plan = plan
        .clone()
        .transform_up(|p| match p.as_ref() {
            LocalPhysicalPlan::PlaceholderScan(placeholder) => {
                let empty_scan = LocalPhysicalPlan::empty_scan(placeholder.schema.clone());
                Ok(Transformed::yes(empty_scan))
            }
            _ => Ok(Transformed::no(p)),
        })?
        .data;

    let psets = HashMap::new();
    let task = SwordfishTask::new(transformed_plan, config, psets, SchedulingStrategy::Spread);
    Ok(task)
}
