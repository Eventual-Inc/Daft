use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use futures::StreamExt;

use super::{DistributedPipelineNode, MaterializedOutput, PipelineOutput, RunningPipelineNode};
use crate::{
    pipeline_node::{DistributedPipelineNodeContext, NodeID},
    plan::PlanID,
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, SwordfishTaskInput},
    },
    stage::{StageContext, StageID},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct IntermediateNode {
    context: DistributedPipelineNodeContext,
    config: Arc<DaftExecutionConfig>,
    plan: LocalPhysicalPlanRef,
    child: Arc<dyn DistributedPipelineNode>,
}

impl IntermediateNode {
    const NODE_NAME: &'static str = "IntermediateNode";

    pub fn new(
        plan_id: PlanID,
        stage_id: StageID,
        node_id: NodeID,
        config: Arc<DaftExecutionConfig>,
        plan: LocalPhysicalPlanRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        let mut context =
            DistributedPipelineNodeContext::new(plan_id, stage_id, node_id, Self::NODE_NAME);
        context.insert("child_name", child.name().to_string());
        context.insert("child_id", child.node_id().to_string());
        Self {
            context,
            config,
            plan,
            child,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        input: RunningPipelineNode,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
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
                    let task = self.make_task_for_materialized_output(materialized_output)?;
                    if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                        break;
                    }
                }
                PipelineOutput::Task(task) => {
                    // append plan to this task
                    let task = self.append_plan_to_task(task)?;
                    if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn make_task_for_materialized_output(
        &self,
        materialized_output: MaterializedOutput,
    ) -> DaftResult<SubmittableTask<SwordfishTask>> {
        let (partition_ref, worker_id) = materialized_output.into_inner();

        let task = SwordfishTask::new(
            self.plan.clone(),
            self.config.clone(),
            SwordfishTaskInput::InMemory(vec![partition_ref]),
            SchedulingStrategy::WorkerAffinity {
                worker_id,
                soft: false,
            },
            self.context.clone().into(),
        );
        Ok(SubmittableTask::new(task))
    }

    fn append_plan_to_task(
        &self,
        submittable_task: SubmittableTask<SwordfishTask>,
    ) -> DaftResult<SubmittableTask<SwordfishTask>> {
        let transformed_plan = self
            .plan
            .clone()
            .transform_up(|p| match p.as_ref() {
                LocalPhysicalPlan::PlaceholderScan(_) => {
                    Ok(Transformed::yes(submittable_task.task().plan.clone()))
                }
                _ => Ok(Transformed::no(p)),
            })?
            .data;
        let scheduling_strategy = submittable_task.task().strategy.clone();
        let inputs = submittable_task.task().inputs.clone();

        let task = submittable_task.with_new_task(SwordfishTask::new(
            transformed_plan,
            self.config.clone(),
            inputs,
            scheduling_strategy,
            self.context.clone().into(),
        ));
        Ok(task)
    }
}

impl TreeDisplay for IntermediateNode {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        writeln!(display, "{}", self.name()).unwrap();
        writeln!(display, "Node ID: {}", self.node_id()).unwrap();
        writeln!(
            display,
            "Local Physical Plan: {}",
            self.plan.single_line_display()
        )
        .unwrap();
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.name().to_string()
    }
}

impl DistributedPipelineNode for IntermediateNode {
    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageContext) -> RunningPipelineNode {
        let input_node = self.child.clone().start(stage_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(input_node, result_tx);
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
