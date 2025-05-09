use std::collections::HashMap;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, InMemoryInfo};

use super::{
    translate::PipelinePlan, DistributedPipelineNode, PipelineInput, PipelineOutput,
    RunningPipelineNode,
};
use crate::{
    channel::{create_channel, Sender},
    scheduling::{
        dispatcher::TaskDispatcherHandle,
        task::{SchedulingStrategy, SwordfishTask},
    },
    stage::StageContext,
};

#[allow(dead_code)]
pub(crate) struct CollectNode {
    plan: PipelinePlan,
    children: Vec<Box<dyn DistributedPipelineNode>>,
}

impl CollectNode {
    #[allow(dead_code)]
    pub fn new(plan: PipelinePlan, children: Vec<Box<dyn DistributedPipelineNode>>) -> Self {
        Self { plan, children }
    }

    async fn source_execution_loop(
        plan: LocalPhysicalPlanRef,
        input: PipelineInput,
        result_tx: Sender<PipelineOutput>,
    ) -> DaftResult<()> {
        match input {
            PipelineInput::InMemorySource {
                cache_key,
                partition_refs,
            } => {
                for partition_ref in partition_refs {
                    let cache_key = cache_key.clone();
                    let transformed_plan = plan
                        .clone()
                        .transform_up(|p| match p.as_ref() {
                            LocalPhysicalPlan::PlaceholderScan(placeholder) => {
                                let size_bytes = partition_ref.size_bytes()?.unwrap();
                                let num_rows = partition_ref.num_rows()?;
                                let in_memory_info = InMemoryInfo::new(
                                    placeholder.schema.clone(),
                                    cache_key.clone(),
                                    None,
                                    1,
                                    size_bytes,
                                    num_rows,
                                    None,
                                    None,
                                );
                                let in_memory = LocalPhysicalPlan::in_memory_scan(
                                    in_memory_info,
                                    StatsState::NotMaterialized,
                                );
                                Ok(Transformed::yes(in_memory))
                            }
                            _ => Ok(Transformed::no(p)),
                        })?
                        .data;
                    let mut psets = HashMap::new();
                    psets.insert(cache_key, vec![partition_ref]);
                    let task =
                        SwordfishTask::new(transformed_plan, psets, SchedulingStrategy::Spread);
                    if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                        break;
                    }
                }
            }
            PipelineInput::ScanTasks {
                scan_tasks,
                pushdowns,
                ..
            } => {
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
                    let task =
                        SwordfishTask::new(transformed_plan, psets, SchedulingStrategy::Spread);
                    if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                        break;
                    }
                }
            }
            PipelineInput::Intermediate => todo!(),
        }
        Ok(())
    }

    #[allow(dead_code)]
    async fn execution_loop(
        task_dispatcher_handle: TaskDispatcherHandle,
        plan: PipelinePlan,
        input_node: Option<RunningPipelineNode>,
        result_tx: Sender<PipelineOutput>,
    ) -> DaftResult<()> {
        match input_node {
            Some(input_node) => {
                todo!("FLOTILLA_MS1: Implement collect execution loop with input node");
            }
            None => Self::source_execution_loop(plan.local_plan, plan.input, result_tx).await,
        }
    }
}

impl TreeDisplay for CollectNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.name()).unwrap();
        println!("display: {:?}", display);
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children()
            .iter()
            .map(|v| v.as_tree_display())
            .collect()
    }
}

impl DistributedPipelineNode for CollectNode {
    fn name(&self) -> &'static str {
        "Collect"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        self.children.iter().map(|c| c.as_ref()).collect()
    }

    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let task_dispatcher_handle = stage_context.get_task_dispatcher_handle();
        let input_node = if let Some(mut input_node) = self.children.pop() {
            assert!(self.children.is_empty());
            let input_running_node = input_node.start(stage_context);
            Some(input_running_node)
        } else {
            None
        };
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = Self::execution_loop(
            task_dispatcher_handle,
            self.plan.clone(),
            input_node,
            result_tx,
        );
        stage_context.spawn_task_on_joinset(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
