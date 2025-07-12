use std::{cmp::Ordering, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{
    make_new_task_from_materialized_outputs, DistributedPipelineNode, PipelineOutput,
    RunningPipelineNode,
};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct LimitNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    limit: usize,
    child: Arc<dyn DistributedPipelineNode>,
}

impl LimitNode {
    const NODE_NAME: NodeName = "Limit";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        limit: usize,
        schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id()],
            vec![child.name()],
            logical_node_id,
        );
        let config = PipelineNodeConfig::new(
            schema,
            stage_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            limit,
            child,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        input: RunningPipelineNode,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        let mut remaining_limit = self.limit;
        let mut materialized_result_stream = input.materialize(scheduler_handle.clone());

        while let Some(materialized_output) = materialized_result_stream.next().await {
            let materialized_output = materialized_output?;

            for next_input in materialized_output.split_into_materialized_outputs() {
                let num_rows = next_input.num_rows()?;

                let (to_send, should_break) = match num_rows.cmp(&remaining_limit) {
                    Ordering::Less => {
                        remaining_limit -= num_rows;
                        (PipelineOutput::Materialized(next_input), false)
                    }
                    Ordering::Equal => (PipelineOutput::Materialized(next_input), true),
                    Ordering::Greater => {
                        let task = make_new_task_from_materialized_outputs(
                            TaskContext::from((&self.context, task_id_counter.next())),
                            vec![next_input],
                            &(self.clone() as Arc<dyn DistributedPipelineNode>),
                            &move |input| {
                                Ok(LocalPhysicalPlan::limit(
                                    input,
                                    remaining_limit as u64,
                                    StatsState::NotMaterialized,
                                ))
                            },
                        )?;
                        (PipelineOutput::Task(task), true)
                    }
                };
                if result_tx.send(to_send).await.is_err() {
                    return Ok(());
                }
                if should_break {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Limit: {}", self.limit)]
    }
}

impl TreeDisplay for LimitNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.context.node_name).unwrap();
            }
            _ => {
                let multiline_display = self.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.context.node_name.to_string()
    }
}

impl DistributedPipelineNode for LimitNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageExecutionContext) -> RunningPipelineNode {
        let input_node = self.child.clone().start(stage_context);

        let limit = self.limit as u64;
        let local_limit_node =
            input_node.pipeline_instruction(stage_context, self.clone(), move |input_plan| {
                Ok(LocalPhysicalPlan::limit(
                    input_plan,
                    limit,
                    StatsState::NotMaterialized,
                ))
            });

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            local_limit_node,
            result_tx,
            stage_context.scheduler_handle(),
            stage_context.task_id_counter(),
        );
        stage_context.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
