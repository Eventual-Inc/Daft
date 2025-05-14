use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use super::{
    materialize::materialize_running_pipeline_outputs, translate::PipelinePlan,
    DistributedPipelineNode, PipelineInput, PipelineOutput, RunningPipelineNode,
};
use crate::{
    scheduling::task::{SchedulingStrategy, SwordfishTask},
    stage::StageContext,
    utils::channel::{create_channel, Sender},
};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct CollectNode {
    node_id: usize,
    config: Arc<DaftExecutionConfig>,
    plan: PipelinePlan,
    children: Vec<Box<dyn DistributedPipelineNode>>,
}

impl CollectNode {
    pub fn new(
        node_id: usize,
        config: Arc<DaftExecutionConfig>,
        plan: PipelinePlan,
        children: Vec<Box<dyn DistributedPipelineNode>>,
    ) -> Self {
        Self {
            node_id,
            config,
            plan,
            children,
        }
    }

    async fn source_execution_loop(
        plan: LocalPhysicalPlanRef,
        config: Arc<DaftExecutionConfig>,
        input: PipelineInput,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> DaftResult<()> {
        match input {
            PipelineInput::InMemorySource { info } => {
                let mut tasks = Vec::new();
                let partition_refs = psets.get(&info.cache_key).unwrap().clone();

                for partition_ref in partition_refs {
                    let task = make_task_for_partition_ref(
                        plan.clone(),
                        partition_ref,
                        info.cache_key.clone(),
                        config.clone(),
                    )?;
                    tasks.push(task);
                }
                let _ = result_tx.send(PipelineOutput::Tasks(tasks)).await;
            }
            PipelineInput::ScanTasks {
                scan_tasks,
                pushdowns,
                ..
            } => {
                let mut tasks = Vec::new();
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
                    tasks.push(task);
                }
                let _ = result_tx.send(PipelineOutput::Tasks(tasks)).await;
            }
            PipelineInput::Intermediate => todo!(),
        }
        Ok(())
    }

    async fn intermediate_execution_loop(
        node_id: usize,
        config: Arc<DaftExecutionConfig>,
        plan: LocalPhysicalPlanRef,
        input: RunningPipelineNode<SwordfishTask>,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
    ) -> DaftResult<()> {
        let mut task_or_partition_ref_stream = materialize_running_pipeline_outputs(input);
        while let Some(result) = task_or_partition_ref_stream.next().await {
            let pipeline_output = result?;
            match pipeline_output {
                PipelineOutput::Running(_) => {
                    unreachable!("All running tasks should be materialized before this point")
                }
                PipelineOutput::Materialized(partition_ref) => {
                    // make new task for this partition ref
                    let task = make_task_for_partition_ref(
                        plan.clone(),
                        partition_ref,
                        node_id.to_string(),
                        config.clone(),
                    )?;
                    if result_tx
                        .send(PipelineOutput::Tasks(vec![task]))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                PipelineOutput::Tasks(tasks) => {
                    // append plan to this task
                    let tasks = tasks
                        .into_iter()
                        .map(|task| append_plan_to_task(task, config.clone(), plan.clone()))
                        .collect::<DaftResult<Vec<_>>>()?;
                    let _ = result_tx.send(PipelineOutput::Tasks(tasks)).await;
                }
            }
        }
        Ok(())
    }

    async fn execution_loop(
        node_id: usize,
        config: Arc<DaftExecutionConfig>,
        plan: PipelinePlan,
        input_node: Option<RunningPipelineNode<SwordfishTask>>,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> DaftResult<()> {
        match input_node {
            Some(input_node) => {
                Self::intermediate_execution_loop(
                    node_id,
                    config,
                    plan.local_plan,
                    input_node,
                    result_tx,
                )
                .await
            }
            None => {
                Self::source_execution_loop(plan.local_plan, config, plan.input, result_tx, psets)
                    .await
            }
        }
    }
}

#[typetag::serde(name = "CollectNode")]
impl DistributedPipelineNode for CollectNode {
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }

    fn name(&self) -> &'static str {
        "Collect"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        self.children.iter().map(|child| child.as_ref()).collect()
    }

    fn start(
        &self,
        stage_context: &mut StageContext,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> RunningPipelineNode<SwordfishTask> {
        let input_node = if let Some(input_node) = self.children.first() {
            let input_running_node = input_node.start(stage_context, psets.clone());
            Some(input_running_node)
        } else {
            None
        };
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = Self::execution_loop(
            self.node_id,
            self.config.clone(),
            self.plan.clone(),
            input_node,
            result_tx,
            psets,
        );
        stage_context.spawn_task_on_joinset(execution_loop);

        RunningPipelineNode::new(result_rx)
    }
}

impl TreeDisplay for CollectNode {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", "Collect").unwrap();
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children()
            .iter()
            .map(|v| v.as_tree_display())
            .collect()
    }
}

fn make_task_for_partition_ref(
    plan: LocalPhysicalPlanRef,
    partition_ref: PartitionRef,
    cache_key: String,
    config: Arc<DaftExecutionConfig>,
) -> DaftResult<SwordfishTask> {
    let info = InMemoryInfo::new(
        plan.schema().clone(),
        cache_key.clone(),
        None,
        1,
        partition_ref.size_bytes()?.unwrap(),
        partition_ref.num_rows()?,
        None,
        None,
    );
    let in_memory_source = LocalPhysicalPlan::in_memory_scan(info, StatsState::NotMaterialized);
    // the first operator of physical_plan has to be a scan
    let transformed_plan = plan
        .transform_up(|p| match p.as_ref() {
            LocalPhysicalPlan::PlaceholderScan(_) => Ok(Transformed::yes(in_memory_source.clone())),
            _ => Ok(Transformed::no(p)),
        })?
        .data;
    let mut psets = HashMap::new();
    psets.insert(cache_key, vec![partition_ref]);
    let task = SwordfishTask::new(transformed_plan, config, psets, SchedulingStrategy::Spread);
    Ok(task)
}

fn append_plan_to_task(
    task: SwordfishTask,
    config: Arc<DaftExecutionConfig>,
    plan: LocalPhysicalPlanRef,
) -> DaftResult<SwordfishTask> {
    let transformed_plan = plan
        .transform_up(|p| match p.as_ref() {
            LocalPhysicalPlan::PlaceholderScan(_) => Ok(Transformed::yes(task.plan().clone())),
            _ => Ok(Transformed::no(p)),
        })?
        .data;
    let task = SwordfishTask::new(
        transformed_plan,
        config,
        Default::default(),
        SchedulingStrategy::Spread,
    );
    Ok(task)
}
