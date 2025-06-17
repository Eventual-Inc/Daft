use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use futures::StreamExt;

use super::{DistributedPipelineNode, MaterializedOutput, PipelineOutput, RunningPipelineNode};
use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask},
    },
    stage::{StageContext, StageID},
    utils::channel::{create_channel, Sender},
    PipelineNodeSpan,
};

pub(crate) struct IntermediateNode {
    plan_id: PlanID,
    stage_id: StageID,
    node_id: NodeID,
    config: Arc<DaftExecutionConfig>,
    plan: LocalPhysicalPlanRef,
    child: Arc<dyn DistributedPipelineNode>,
}

impl IntermediateNode {
    pub fn new(
        plan_id: PlanID,
        stage_id: StageID,
        node_id: NodeID,
        config: Arc<DaftExecutionConfig>,
        plan: LocalPhysicalPlanRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        Self {
            plan_id,
            stage_id,
            node_id,
            config,
            plan,
            child,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
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
                    let task = self
                        .make_task_for_materialized_output(materialized_output, context.clone())?;
                    if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                        break;
                    }
                }
                PipelineOutput::Task(task) => {
                    // append plan to this task
                    let task = self.append_plan_to_task(task, context.clone())?;
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
        context: HashMap<String, String>,
    ) -> DaftResult<SubmittableTask<SwordfishTask>> {
        let (partition_ref, worker_id) = materialized_output.into_inner();

        let info = InMemoryInfo::new(
        self.plan.schema().clone(),
        self.node_id.to_string(),
        None,
        1,
        partition_ref.size_bytes()?.expect("make_task_for_materialized_output: Expect that the input partition ref for an intermediate node has a known size"),
        partition_ref.num_rows()?,
        None,
        None,
    );
        let in_memory_source = LocalPhysicalPlan::in_memory_scan(info, StatsState::NotMaterialized);
        // the first operator of physical_plan has to be a scan
        let transformed_plan = self
            .plan
            .clone()
            .transform_up(|p| match p.as_ref() {
                LocalPhysicalPlan::PlaceholderScan(_) => {
                    Ok(Transformed::yes(in_memory_source.clone()))
                }
                _ => Ok(Transformed::no(p)),
            })?
            .data;
        let psets = HashMap::from([(self.node_id.to_string(), vec![partition_ref])]);

        let task = SwordfishTask::new(
            transformed_plan,
            self.config.clone(),
            psets,
            SchedulingStrategy::WorkerAffinity {
                worker_id,
                soft: false,
            },
            context,
            self.node_id,
        );
        Ok(SubmittableTask::new(task))
    }

    fn append_plan_to_task(
        &self,
        submittable_task: SubmittableTask<SwordfishTask>,
        context: HashMap<String, String>,
    ) -> DaftResult<SubmittableTask<SwordfishTask>> {
        let transformed_plan = self
            .plan
            .clone()
            .transform_up(|p| match p.as_ref() {
                LocalPhysicalPlan::PlaceholderScan(_) => {
                    Ok(Transformed::yes(submittable_task.task().plan()))
                }
                _ => Ok(Transformed::no(p)),
            })?
            .data;
        let scheduling_strategy = submittable_task.task().strategy().clone();
        let psets = submittable_task.task().psets().clone();

        let task = submittable_task.with_new_task(SwordfishTask::new(
            transformed_plan,
            self.config.clone(),
            psets,
            scheduling_strategy,
            context,
            self.node_id,
        ));
        Ok(task)
    }
}

impl TreeDisplay for IntermediateNode {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        writeln!(display, "{}", self.name()).unwrap();
        writeln!(display, "Node ID: {}", self.node_id).unwrap();
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
    fn name(&self) -> &'static str {
        "DistributedIntermediateNode"
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageContext) -> RunningPipelineNode {
        let span = PipelineNodeSpan::new(self.clone(), stage_context.span.hooks_manager.clone());
        let context = {
            let child_name = self.child.name();
            let child_id = self.child.node_id();

            HashMap::from([
                ("plan_id".to_string(), self.plan_id.to_string()),
                ("stage_id".to_string(), format!("{}", self.stage_id)),
                ("node_id".to_string(), format!("{}", self.node_id)),
                ("node_name".to_string(), self.name().to_string()),
                ("child_id".to_string(), format!("{}", child_id)),
                ("child_name".to_string(), child_name.to_string()),
            ])
        };

        let input_node = self.child.clone().start(stage_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(input_node, result_tx, context);
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx, span)
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

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
