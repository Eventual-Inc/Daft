use std::{cmp::Ordering, collections::VecDeque, sync::Arc};

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

    fn process_materialized_output(
        self: &Arc<Self>,
        materialized_output: MaterializedOutput,
        remaining_limit: &mut usize,
        task_id_counter: &TaskIDCounter,
    ) -> DaftResult<Vec<SubmittableTask<SwordfishTask>>> {
        let mut downstream_tasks = vec![];
        for next_input in materialized_output.split_into_materialized_outputs() {
            let num_rows = next_input.num_rows()?;

            let task = match num_rows.cmp(remaining_limit) {
                Ordering::Less => {
                    *remaining_limit -= num_rows;
                    let task = make_in_memory_scan_from_materialized_outputs(
                        TaskContext::from((&self.context, task_id_counter.next())),
                        vec![next_input],
                        &(self.clone() as Arc<dyn DistributedPipelineNode>),
                    )?;
                    task
                }
                Ordering::Equal => {
                    *remaining_limit = 0;
                    let task = make_in_memory_scan_from_materialized_outputs(
                        TaskContext::from((&self.context, task_id_counter.next())),
                        vec![next_input],
                        &(self.clone() as Arc<dyn DistributedPipelineNode>),
                    )?;
                    task
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
                                None, // TODO(zhenchao) support offset
                                StatsState::NotMaterialized,
                            )
                        },
                    )?;
                    *remaining_limit = 0;
                    task
                }
            };
            downstream_tasks.push(task);
            if *remaining_limit == 0 {
                break;
            }
        }
        Ok(downstream_tasks)
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
                        None,
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

            let downstream_tasks = self.process_materialized_output(
                materialized_output,
                &mut remaining_limit,
                &task_id_counter,
            )?;
            for task in downstream_tasks {
                if result_tx.send(task).await.is_err() {
                    return Ok(());
                }
            }
            if remaining_limit == 0 {
                return Ok(());
            }
        }

        Ok(())
    }

    async fn execution_loop(
        self: Arc<Self>,
        mut input: SubmittableTaskStream,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        let mut remaining_limit = self.limit;
        let mut max_concurrent_tasks = 1;
        let mut input_exhausted = false;

        // Keep submitting local limit tasks as long as we have remaining limit or we have input
        while !input_exhausted {
            let mut local_limits = VecDeque::with_capacity(max_concurrent_tasks);
            let local_limit_per_task = remaining_limit;

            // Submit tasks until we have max_concurrent_tasks or we run out of input
            for _ in 0..max_concurrent_tasks {
                if let Some(task) = input.next().await {
                    let task_with_limit = append_plan_to_existing_task(
                        task,
                        &(self.clone() as Arc<dyn DistributedPipelineNode>),
                        &move |input| {
                            LocalPhysicalPlan::limit(
                                input,
                                local_limit_per_task as u64,
                                None,
                                StatsState::NotMaterialized,
                            )
                        },
                    );
                    let future = task_with_limit.submit(&scheduler_handle)?;
                    local_limits.push_back(future);
                } else {
                    input_exhausted = true;
                    break;
                }
            }

            let mut total_num_rows = 0;
            // Process results from all local limit tasks
            let mut downstream_tasks = vec![];
            for future in local_limits.drain(..) {
                let maybe_result = future.await?;
                if let Some(materialized_output) = maybe_result {
                    total_num_rows += materialized_output.num_rows()?;
                    // Process the result and check if we should exit early
                    downstream_tasks.extend(self.process_materialized_output(
                        materialized_output,
                        &mut remaining_limit,
                        &task_id_counter,
                    )?);
                }
            }

            // Send tasks to result channel
            for task in downstream_tasks {
                if result_tx.send(task).await.is_err() {
                    return Ok(());
                }
            }

            // Update max_concurrent_tasks based on actual output
            if total_num_rows > 0 {
                let rows_per_task = total_num_rows / max_concurrent_tasks;
                let tasks_needed = (remaining_limit + rows_per_task - 1) / rows_per_task;
                max_concurrent_tasks = tasks_needed.max(1);
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
                    // TODO(zhenchao) support offset
                    LocalPhysicalPlan::limit(input_plan, limit, None, StatsState::NotMaterialized)
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
