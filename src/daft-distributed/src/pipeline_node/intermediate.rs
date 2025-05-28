use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use futures::StreamExt;

use super::{DistributedPipelineNode, MaterializedOutput, PipelineOutput, RunningPipelineNode};
use crate::{
    scheduling::task::{SchedulingStrategy, SwordfishTask},
    stage::StageContext,
    utils::channel::{create_channel, Sender},
};

#[allow(dead_code)]
pub(crate) struct IntermediateNode {
    node_id: usize,
    config: Arc<DaftExecutionConfig>,
    plan: LocalPhysicalPlanRef,
    children: Vec<Box<dyn DistributedPipelineNode>>,
}

impl IntermediateNode {
    #[allow(dead_code)]
    pub fn new(
        node_id: usize,
        config: Arc<DaftExecutionConfig>,
        plan: LocalPhysicalPlanRef,
        children: Vec<Box<dyn DistributedPipelineNode>>,
    ) -> Self {
        Self {
            node_id,
            config,
            plan,
            children,
        }
    }

    async fn execution_loop(
        node_id: usize,
        config: Arc<DaftExecutionConfig>,
        plan: LocalPhysicalPlanRef,
        input: RunningPipelineNode,
        result_tx: Sender<PipelineOutput>,
    ) -> DaftResult<()> {
        let mut task_or_partition_ref_stream = input.into_stream();

        while let Some(pipeline_output) = task_or_partition_ref_stream.next().await {
            match pipeline_output {
                PipelineOutput::Running(_) => {
                    unreachable!("All running tasks should be materialized before this point")
                }
                PipelineOutput::Materialized(materialized_output) => {
                    // make new task for this partition ref
                    let task = make_task_for_materialized_output(
                        plan.clone(),
                        materialized_output,
                        node_id.to_string(),
                        config.clone(),
                    )?;
                    if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                        break;
                    }
                }
                PipelineOutput::Task(task) => {
                    // append plan to this task
                    let task = append_plan_to_task(task, config.clone(), plan.clone())?;
                    if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

impl DistributedPipelineNode for IntermediateNode {
    fn name(&self) -> &'static str {
        "Intermediate"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        self.children.iter().map(|child| child.as_ref()).collect()
    }

    fn start(
        &mut self,
        stage_context: &mut StageContext,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> RunningPipelineNode {
        let input_node = self
            .children
            .first_mut()
            .unwrap()
            .start(stage_context, psets);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = Self::execution_loop(
            self.node_id,
            self.config.clone(),
            self.plan.clone(),
            input_node,
            result_tx,
        );
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }
}

fn make_task_for_materialized_output(
    plan: LocalPhysicalPlanRef,
    materialized_output: MaterializedOutput,
    cache_key: String,
    config: Arc<DaftExecutionConfig>,
) -> DaftResult<SwordfishTask> {
    let (partition_ref, worker_id) = materialized_output.into_inner();

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
    let task = SwordfishTask::new(
        transformed_plan,
        config,
        psets,
        SchedulingStrategy::NodeAffinity {
            node_id: worker_id.to_string(),
            soft: true,
        },
    );
    Ok(task)
}

fn append_plan_to_task(
    task: SwordfishTask,
    config: Arc<DaftExecutionConfig>,
    plan: LocalPhysicalPlanRef,
) -> DaftResult<SwordfishTask> {
    let transformed_plan = plan
        .transform_up(|p| match p.as_ref() {
            LocalPhysicalPlan::PlaceholderScan(_) => Ok(Transformed::yes(task.plan())),
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
