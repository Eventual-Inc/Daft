use std::{cmp::Ordering, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{
    make_new_task_from_materialized_outputs, DistributedPipelineNode, MaterializedOutput,
    SubmittableTaskStream,
};
use crate::{
    pipeline_node::{
        append_plan_to_existing_task, make_in_memory_scan_from_materialized_outputs, NodeID,
        NodeName, PipelineNodeConfig, PipelineNodeContext,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct LimitNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    limit: usize,
    eager: bool,
    child: Arc<dyn DistributedPipelineNode>,
}

impl LimitNode {
    const NODE_NAME: NodeName = "Limit";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        limit: usize,
        eager: bool,
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
            eager,
            child,
        }
    }

    async fn process_materialized_output(
        self: &Arc<Self>,
        materialized_output: MaterializedOutput,
        remaining_limit: &mut usize,
        result_tx: &Sender<SubmittableTask<SwordfishTask>>,
        task_id_counter: &TaskIDCounter,
    ) -> DaftResult<bool> {
        for next_input in materialized_output.split_into_materialized_outputs() {
            let num_rows = next_input.num_rows()?;

            let (to_send, should_break) = match num_rows.cmp(remaining_limit) {
                Ordering::Less => {
                    *remaining_limit -= num_rows;
                    let task = make_in_memory_scan_from_materialized_outputs(
                        TaskContext::from((&self.context, task_id_counter.next())),
                        vec![next_input],
                        &(self.clone() as Arc<dyn DistributedPipelineNode>),
                    )?;
                    (task, false)
                }
                Ordering::Equal => {
                    let task = make_in_memory_scan_from_materialized_outputs(
                        TaskContext::from((&self.context, task_id_counter.next())),
                        vec![next_input],
                        &(self.clone() as Arc<dyn DistributedPipelineNode>),
                    )?;
                    (task, true)
                }
                Ordering::Greater => {
                    let remaining = *remaining_limit;
                    let task = make_new_task_from_materialized_outputs(
                        TaskContext::from((&self.context, task_id_counter.next())),
                        vec![next_input],
                        &(self.clone() as Arc<dyn DistributedPipelineNode>),
                        move |input| {
                            LocalPhysicalPlan::limit(
                                input,
                                remaining as u64,
                                StatsState::NotMaterialized,
                            )
                        },
                    )?;
                    (task, true)
                }
            };
            if result_tx.send(to_send).await.is_err() {
                return Ok(true); // Channel closed, should break
            }
            if should_break {
                return Ok(true); // Limit reached, should break
            }
        }
        Ok(false) // Continue processing
    }

    async fn eager_execution_loop(
        self: Arc<Self>,
        mut input: SubmittableTaskStream,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        let mut remaining_limit = self.limit;
        while let Some(task) = input.next().await {
            let limit_for_task = remaining_limit;
            let task_with_limit = append_plan_to_existing_task(
                task,
                &(self.clone() as Arc<dyn DistributedPipelineNode>),
                &move |input| {
                    LocalPhysicalPlan::limit(
                        input,
                        limit_for_task as u64,
                        StatsState::NotMaterialized,
                    )
                },
            );

            let maybe_result = task_with_limit.submit(&scheduler_handle)?.await?;
            let materialized_output = if let Some(output) = maybe_result {
                output
            } else {
                continue;
            };

            if self
                .process_materialized_output(
                    materialized_output,
                    &mut remaining_limit,
                    &result_tx,
                    &task_id_counter,
                )
                .await?
            {
                return Ok(());
            }
        }

        Ok(())
    }

    async fn execution_loop(
        self: Arc<Self>,
        input: SubmittableTaskStream,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        let mut remaining_limit = self.limit;
        let mut materialized_result_stream = input.materialize(scheduler_handle.clone());

        while let Some(materialized_output) = materialized_result_stream.next().await {
            let materialized_output = materialized_output?;

            if self
                .process_materialized_output(
                    materialized_output,
                    &mut remaining_limit,
                    &result_tx,
                    &task_id_counter,
                )
                .await?
            {
                return Ok(());
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

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let input_stream = self.child.clone().produce_tasks(stage_context);
        let (result_tx, result_rx) = create_channel(1);

        let limit = self.limit as u64;
        if self.eager {
            let execution_loop = self.eager_execution_loop(
                input_stream,
                result_tx,
                stage_context.scheduler_handle(),
                stage_context.task_id_counter(),
            );
            stage_context.spawn(execution_loop);
        } else {
            let local_limit_stream =
                input_stream.pipeline_instruction(self.clone(), move |input_plan| {
                    LocalPhysicalPlan::limit(input_plan, limit, StatsState::NotMaterialized)
                });
            let execution_loop = self.execution_loop(
                local_limit_stream,
                result_tx,
                stage_context.scheduler_handle(),
                stage_context.task_id_counter(),
            );
            stage_context.spawn(execution_loop);
        };

        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
