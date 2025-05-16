use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use futures::StreamExt;

use super::{
    materialize::materialize_running_pipeline_outputs, DistributedPipelineNode, MaterializedOutput,
    PipelineOutput, RunningPipelineNode,
};
use crate::{
    scheduling::task::{SchedulingStrategy, SwordfishTask},
    stage::StageContext,
    utils::channel::{create_channel, Sender},
};

#[derive(Debug)]
pub(crate) struct ScanSourceNode {
    node_id: usize,
    config: Arc<DaftExecutionConfig>,
    plan: LocalPhysicalPlanRef,
    pushdowns: Pushdowns,
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
}

impl ScanSourceNode {
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
        for scan_task in scan_tasks.iter() {
            let transformed_plan = plan
                .clone()
                .transform_up(|p| match p.as_ref() {
                    LocalPhysicalPlan::PlaceholderScan(placeholder) => {
                        let physical_scan = LocalPhysicalPlan::physical_scan(
                            vec![scan_task.clone()].into(),
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
            let task = SwordfishTask::new(
                transformed_plan,
                config.clone(),
                psets,
                SchedulingStrategy::Spread,
            );
            if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                break;
            }
        }

        Ok(())
    }
}

impl DistributedPipelineNode for ScanSourceNode {
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }

    fn name(&self) -> &'static str {
        "ScanSource"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![]
    }

    fn start(
        &self,
        stage_context: &mut StageContext,
        _psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> RunningPipelineNode<SwordfishTask> {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = Self::execution_loop(
            self.plan.clone(),
            self.config.clone(),
            self.pushdowns.clone(),
            self.scan_tasks.clone(),
            result_tx,
        );
        stage_context.spawn_task_on_joinset(execution_loop);

        RunningPipelineNode::new(result_rx)
    }
}

impl TreeDisplay for ScanSourceNode {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", "ScanSource").unwrap();
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}
