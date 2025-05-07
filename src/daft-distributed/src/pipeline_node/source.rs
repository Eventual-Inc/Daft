use std::collections::HashMap;

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{
    stats::{ApproxStats, PlanStats, StatsState},
    InMemoryInfo,
};

use super::{
    translate::PipelinePlan, DistributedPipelineNode, PipelineInput, PipelineOutput,
    RunningPipelineNode,
};
use crate::{
    channel::{create_channel, Sender},
    scheduling::{dispatcher::TaskDispatcherHandle, task::SwordfishTask},
    stage::StageContext,
};

#[allow(dead_code)]
pub(crate) struct SourceNode {
    plan: LocalPhysicalPlanRef,
    input: PipelineInput,
}

impl SourceNode {
    #[allow(dead_code)]
    pub fn new(plan: LocalPhysicalPlanRef, input: PipelineInput) -> Self {
        Self { plan, input }
    }

    #[allow(dead_code)]
    async fn execution_loop(
        task_dispatcher_handle: TaskDispatcherHandle,
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
                    let task = SwordfishTask::new(transformed_plan, psets);
                    if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                        break;
                    }
                }
            }
            PipelineInput::ScanTasks {
                source_id,
                scan_tasks,
                pushdowns,
            } => {
                for scan_task in scan_tasks.iter() {
                    let transformed_plan = plan.clone().transform_up(|p| match p.as_ref() {
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
                    })?;
                    let psets = HashMap::new();
                    let task = SwordfishTask::new(transformed_plan.data, psets);
                    if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                        break;
                    }
                }
            }
            PipelineInput::Intermediate => todo!(),
        }
        Ok(())
    }
}

impl DistributedPipelineNode for SourceNode {
    fn name(&self) -> &'static str {
        "Source"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![]
    }

    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let task_dispatcher_handle = stage_context.get_task_dispatcher_handle();
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = Self::execution_loop(
            task_dispatcher_handle,
            self.plan.clone(),
            self.input.clone(),
            result_tx,
        );
        stage_context.spawn_task_on_joinset(execution_loop);

        RunningPipelineNode::new(result_rx)
    }
}
