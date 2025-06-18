use std::{cmp::Ordering, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{DistributedPipelineNode, MaterializedOutput, PipelineOutput, RunningPipelineNode};
use crate::{
    pipeline_node::{DistributedPipelineNodeContext, NodeID},
    plan::PlanID,
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SchedulingStrategy, SwordfishTask, SwordfishTaskInput},
    },
    stage::{StageContext, StageID},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct LimitNode {
    context: DistributedPipelineNodeContext,
    limit: usize,
    schema: SchemaRef,
    config: Arc<DaftExecutionConfig>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl LimitNode {
    const NODE_NAME: &'static str = "LimitNode";

    pub fn new(
        plan_id: PlanID,
        stage_id: StageID,
        node_id: NodeID,
        limit: usize,
        schema: SchemaRef,
        config: Arc<DaftExecutionConfig>,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        let mut context =
            DistributedPipelineNodeContext::new(plan_id, stage_id, node_id, Self::NODE_NAME);
        context.insert("child_name", child.name().to_string());
        context.insert("child_id", child.node_id().to_string());
        Self {
            context,
            limit,
            schema,
            config,
            child,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        input: RunningPipelineNode,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let mut remaining_limit = self.limit;
        let mut materialized_result_stream = input.materialize(scheduler_handle.clone());

        while let Some(materialized_output) = materialized_result_stream.next().await {
            let materialized_output = materialized_output?;
            let num_rows = materialized_output.partition().num_rows()?;

            let (to_send, should_break) = match num_rows.cmp(&remaining_limit) {
                Ordering::Less => {
                    remaining_limit -= num_rows;
                    (PipelineOutput::Materialized(materialized_output), false)
                }
                Ordering::Equal => (PipelineOutput::Materialized(materialized_output), true),
                Ordering::Greater => {
                    let task_with_limit =
                        self.make_task_with_limit(materialized_output, remaining_limit)?;
                    let task_result_handle = task_with_limit.submit(&scheduler_handle).await?;
                    (PipelineOutput::Running(task_result_handle), true)
                }
            };
            if result_tx.send(to_send).await.is_err() {
                break;
            }
            if should_break {
                break;
            }
        }
        Ok(())
    }

    fn make_task_with_limit(
        &self,
        materialized_output: MaterializedOutput,
        limit: usize,
    ) -> DaftResult<SubmittableTask<SwordfishTask>> {
        let (partition, worker_id) = materialized_output.into_inner();
        let placeholder =
            LocalPhysicalPlan::placeholder_scan(self.schema.clone(), StatsState::NotMaterialized);

        let limit_plan =
            LocalPhysicalPlan::limit(placeholder, limit as i64, StatsState::NotMaterialized);

        let task = SwordfishTask::new(
            limit_plan,
            self.config.clone(),
            SwordfishTaskInput::InMemory(vec![partition]),
            SchedulingStrategy::WorkerAffinity {
                worker_id,
                soft: true,
            },
            self.context.clone().into(),
        );
        Ok(SubmittableTask::new(task))
    }
}

impl TreeDisplay for LimitNode {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        writeln!(display, "{}", self.name()).unwrap();
        writeln!(display, "Node ID: {}", self.node_id()).unwrap();
        writeln!(display, "Limit: {}", self.limit).unwrap();
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.name().to_string()
    }
}

impl DistributedPipelineNode for LimitNode {
    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageContext) -> RunningPipelineNode {
        let input_node = self.child.clone().start(stage_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            input_node,
            result_tx,
            stage_context.scheduler_handle.clone(),
        );
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
