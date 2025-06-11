use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use futures::StreamExt;

use super::{DistributedPipelineNode, MaterializedOutput, PipelineOutput, RunningPipelineNode};
use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    scheduling::task::{SchedulingStrategy, SwordfishTask},
    stage::{StageContext, StageID},
    utils::channel::{create_channel, Sender},
};

#[allow(dead_code)]
pub(crate) struct IntermediateNode {
    plan_id: PlanID,
    stage_id: StageID,
    node_id: NodeID,
    config: Arc<DaftExecutionConfig>,
    plan: LocalPhysicalPlanRef,
    children: Vec<Box<dyn DistributedPipelineNode>>,
}

impl IntermediateNode {
    #[allow(dead_code)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        plan_id: PlanID,
        stage_id: StageID,
        node_id: NodeID,
        config: Arc<DaftExecutionConfig>,
        plan: LocalPhysicalPlanRef,
        children: Vec<Box<dyn DistributedPipelineNode>>,
    ) -> Self {
        Self {
            plan_id,
            stage_id,
            node_id,
            config,
            plan,
            children,
        }
    }

    async fn execution_loop(
        node_id: NodeID,
        config: Arc<DaftExecutionConfig>,
        plan: LocalPhysicalPlanRef,
        input: RunningPipelineNode,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        context: HashMap<String, String>,
    ) -> DaftResult<()> {
        let mut task_or_partition_ref_stream = input.materialize_running();

        while let Some(pipeline_result) = task_or_partition_ref_stream.next().await {
            let pipeline_output = pipeline_result?;
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
                        context.clone(),
                    )?;
                    if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                        break;
                    }
                }
                PipelineOutput::Task(task) => {
                    // append plan to this task
                    let task =
                        append_plan_to_task(task, config.clone(), plan.clone(), context.clone())?;
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

    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let context = {
            let input = self
                .children
                .first()
                .expect("IntermediateNode::start: IntermediateNode must have at least 1 child");
            let child_name = input.name();
            let child_id = input.node_id();

            HashMap::from([
                ("plan_id".to_string(), self.plan_id.to_string()),
                ("stage_id".to_string(), format!("{}", self.stage_id)),
                ("node_id".to_string(), format!("{}", self.node_id)),
                ("node_name".to_string(), self.name().to_string()),
                ("child_id".to_string(), format!("{}", child_id)),
                ("child_name".to_string(), child_name.to_string()),
            ])
        };

        let input_node = self
            .children
            .first_mut()
            .expect("IntermediateNode::start: IntermediateNode must have at least 1 child")
            .start(stage_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = Self::execution_loop(
            self.node_id,
            self.config.clone(),
            self.plan.clone(),
            input_node,
            result_tx,
            context,
        );
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

fn make_task_for_materialized_output(
    plan: LocalPhysicalPlanRef,
    materialized_output: MaterializedOutput,
    cache_key: String,
    config: Arc<DaftExecutionConfig>,
    context: HashMap<String, String>,
) -> DaftResult<SwordfishTask> {
    let (partition_ref, worker_id) = materialized_output.into_inner();

    let info = InMemoryInfo::new(
        plan.schema().clone(),
        cache_key.clone(),
        None,
        1,
        partition_ref.size_bytes()?.expect("make_task_for_materialized_output: Expect that the input partition ref for an intermediate node has a known size"),
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
    let psets = HashMap::from([(cache_key, vec![partition_ref])]);

    let task = SwordfishTask::new(
        transformed_plan,
        config,
        psets,
        SchedulingStrategy::WorkerAffinity {
            worker_id,
            soft: true,
        },
        context,
    );
    Ok(task)
}

fn append_plan_to_task(
    task: SwordfishTask,
    config: Arc<DaftExecutionConfig>,
    plan: LocalPhysicalPlanRef,
    context: HashMap<String, String>,
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
        context,
    );
    Ok(task)
}
