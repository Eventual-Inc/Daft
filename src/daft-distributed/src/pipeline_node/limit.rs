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
        append_plan_to_existing_task, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
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
    offset: Option<usize>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl LimitNode {
    const NODE_NAME: NodeName = "Limit";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        limit: usize,
        offset: Option<usize>,
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
            offset,
            child,
        }
    }

    fn process_materialized_output(
        self: &Arc<Self>,
        materialized_output: MaterializedOutput,
        remaining_skip: &mut usize,
        remaining_take: &mut usize,
        task_id_counter: &TaskIDCounter,
    ) -> DaftResult<Vec<SubmittableTask<SwordfishTask>>> {
        let mut downstream_tasks = vec![];
        for next_input in materialized_output.split_into_materialized_outputs() {
            let mut num_rows = next_input.num_rows()?;

            let skip_num_rows = (*remaining_skip).min(num_rows);
            if *remaining_skip > 0 {
                *remaining_skip -= skip_num_rows;
                // all input rows are skipped
                if skip_num_rows >= num_rows {
                    continue;
                }

                num_rows -= skip_num_rows;
            }

            let task = match num_rows.cmp(remaining_take) {
                Ordering::Less | Ordering::Equal => {
                    *remaining_take -= num_rows;
                    make_new_task_from_materialized_outputs(
                        TaskContext::from((&self.context, task_id_counter.next())),
                        vec![next_input],
                        &(self.clone() as Arc<dyn DistributedPipelineNode>),
                        move |input| {
                            if skip_num_rows > 0 {
                                LocalPhysicalPlan::limit(
                                    input,
                                    num_rows as u64,
                                    Some(skip_num_rows as u64),
                                    StatsState::NotMaterialized,
                                )
                            } else {
                                input
                            }
                        },
                    )?
                }
                Ordering::Greater => {
                    let remaining = *remaining_take;
                    let task = make_new_task_from_materialized_outputs(
                        TaskContext::from((&self.context, task_id_counter.next())),
                        vec![next_input],
                        &(self.clone() as Arc<dyn DistributedPipelineNode>),
                        move |input| {
                            LocalPhysicalPlan::limit(
                                input,
                                remaining as u64,
                                Some(skip_num_rows as u64),
                                StatsState::NotMaterialized,
                            )
                        },
                    )?;
                    *remaining_take = 0;
                    task
                }
            };
            downstream_tasks.push(task);
            if *remaining_take == 0 {
                break;
            }
        }
        Ok(downstream_tasks)
    }

    async fn limit_execution_loop(
        self: Arc<Self>,
        mut input: SubmittableTaskStream,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        let mut remaining_skip = self.offset.unwrap_or(0);
        let mut remaining_take = self.limit;
        let mut max_concurrent_tasks = 1;
        let mut input_exhausted = false;

        // Keep submitting local limit tasks as long as we have remaining limit or we have input
        while !input_exhausted {
            let mut local_limits = VecDeque::new();
            let local_limit_per_task = remaining_skip + remaining_take;

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
                                Some(0),
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
            let num_local_limits = local_limits.len();
            let mut total_num_rows = 0;
            // Process results from all local limit tasks
            let mut downstream_tasks = vec![];
            for future in local_limits {
                let maybe_result = future.await?;
                if let Some(materialized_output) = maybe_result {
                    total_num_rows += materialized_output.num_rows()?;
                    // Process the result and check if we should exit early
                    downstream_tasks.extend(self.process_materialized_output(
                        materialized_output,
                        &mut remaining_skip,
                        &mut remaining_take,
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
            // Only update if we have remaining limit, and we did get some output
            if remaining_take > 0 && total_num_rows > 0 && num_local_limits > 0 {
                let rows_per_task = total_num_rows.div_ceil(num_local_limits);
                max_concurrent_tasks = remaining_take.div_ceil(rows_per_task);
            }
        }

        Ok(())
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match &self.offset {
            Some(o) => vec![format!("Limit: Num Rows = {}, Offset = {}", self.limit, o)],
            None => vec![format!("Limit: {}", self.limit)],
        }
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

        stage_context.spawn(self.limit_execution_loop(
            input_stream,
            result_tx,
            stage_context.scheduler_handle(),
            stage_context.task_id_counter(),
        ));

        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
