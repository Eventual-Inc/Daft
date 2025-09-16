use std::{cmp::Ordering, collections::VecDeque, sync::Arc};

use common_display::{DisplayLevel, tree::TreeDisplay};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{
    DistributedPipelineNode, MaterializedOutput, SubmittableTaskStream,
    make_new_task_from_materialized_outputs,
};
use crate::{
    pipeline_node::{
        NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext, append_plan_to_existing_task,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{Sender, create_channel},
};

/// Keeps track of the remaining skip and take.
///
/// Skip is the number of rows to skip if there is an offset.
/// Take is the number of rows to take for the limit.
struct LimitState {
    remaining_skip: usize,
    remaining_take: usize,
}

impl LimitState {
    fn new(limit: usize, offset: Option<usize>) -> Self {
        Self {
            remaining_skip: offset.unwrap_or(0),
            remaining_take: limit,
        }
    }

    fn remaining_skip(&self) -> usize {
        self.remaining_skip
    }

    fn remaining_take(&self) -> usize {
        self.remaining_take
    }

    fn decrement_skip(&mut self, amount: usize) {
        self.remaining_skip = self.remaining_skip.saturating_sub(amount);
    }

    fn decrement_take(&mut self, amount: usize) {
        self.remaining_take = self.remaining_take.saturating_sub(amount);
    }

    fn is_skip_done(&self) -> bool {
        self.remaining_skip == 0
    }

    fn is_take_done(&self) -> bool {
        self.remaining_take == 0
    }

    fn total_remaining(&self) -> usize {
        self.remaining_skip + self.remaining_take
    }
}

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
        limit_state: &mut LimitState,
        task_id_counter: &TaskIDCounter,
    ) -> DaftResult<Vec<SubmittableTask<SwordfishTask>>> {
        let mut downstream_tasks = vec![];
        for next_input in materialized_output.split_into_materialized_outputs() {
            let mut num_rows = next_input.num_rows()?;

            let skip_num_rows = limit_state.remaining_skip().min(num_rows);
            if !limit_state.is_skip_done() {
                limit_state.decrement_skip(skip_num_rows);
                // all input rows are skipped
                if skip_num_rows >= num_rows {
                    continue;
                }

                num_rows -= skip_num_rows;
            }

            let task = match num_rows.cmp(&limit_state.remaining_take()) {
                Ordering::Less | Ordering::Equal => {
                    limit_state.decrement_take(num_rows);
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
                        None,
                    )?
                }
                Ordering::Greater => {
                    let remaining = limit_state.remaining_take();
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
                        None,
                    )?;
                    limit_state.decrement_take(remaining);
                    task
                }
            };
            downstream_tasks.push(task);
            if limit_state.is_take_done() {
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
        let mut limit_state = LimitState::new(self.limit, self.offset);
        let mut max_concurrent_tasks = 1;
        let mut input_exhausted = false;

        // Keep submitting local limit tasks as long as we have remaining limit or we have input
        while !input_exhausted {
            let mut local_limits = VecDeque::new();
            let local_limit_per_task = limit_state.total_remaining();

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
            for future in local_limits {
                let maybe_result = future.await?;
                if let Some(materialized_output) = maybe_result {
                    total_num_rows += materialized_output.num_rows()?;
                    // Process the result and get the next tasks
                    let next_tasks = self.process_materialized_output(
                        materialized_output,
                        &mut limit_state,
                        &task_id_counter,
                    )?;
                    // Send the next tasks to the result channel
                    for task in next_tasks {
                        if result_tx.send(task).await.is_err() {
                            return Ok(());
                        }
                    }

                    if limit_state.is_take_done() {
                        break;
                    }
                }
            }

            // Update max_concurrent_tasks based on actual output
            // Only update if we have remaining limit, and we did get some output
            if !limit_state.is_take_done() && total_num_rows > 0 && num_local_limits > 0 {
                let rows_per_task = total_num_rows.div_ceil(num_local_limits);
                max_concurrent_tasks = limit_state.remaining_take().div_ceil(rows_per_task);
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
