use std::{cmp::Ordering, collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use common_metrics::QueryID;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;
use opentelemetry::{global, metrics::Counter};

use super::{
    DistributedPipelineNode, MaterializedOutput, PipelineNodeImpl, SubmittableTaskStream,
    make_empty_scan_task, make_new_task_from_materialized_outputs,
};
use crate::{
    pipeline_node::{
        NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext, append_plan_to_existing_task,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    statistics::{
        TaskEvent,
        stats::{DefaultRuntimeStats, RuntimeStats},
    },
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

pub struct LimitStats {
    default_stats: DefaultRuntimeStats,
    /// Number of rows emitted by the LIMIT, assuming no failed tasks.
    /// TODO: Handle failed tasks by tracking both an active_rows_out and completed_rows_out
    /// active_rows_out is immediately incremented when we produce the related task and pass it downstream.
    /// completed_rows_out only increments when the downstream task completes successfully.
    active_rows_out: Counter<u64>,
}

impl LimitStats {
    fn new(node_id: NodeID, query_id: QueryID) -> Self {
        let meter = global::meter("daft.distributed.node_stats");
        Self {
            default_stats: DefaultRuntimeStats::new_impl(&meter, node_id, query_id),
            active_rows_out: meter
                .u64_counter("daft.distributed.node_stats.active_rows_out")
                .build(),
        }
    }

    fn add_active_rows_out(&self, rows: u64) {
        self.active_rows_out
            .add(rows, self.default_stats.node_kv.as_slice());
    }
}

impl RuntimeStats for LimitStats {
    fn handle_task_event(&self, event: &TaskEvent) -> DaftResult<()> {
        // We currently don't track completion for active_rows_out, so just pass to default stats
        self.default_stats.handle_task_event(event)
    }
}

pub(crate) struct LimitNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    limit: usize,
    offset: Option<usize>,
    child: DistributedPipelineNode,
    stats: Arc<LimitStats>,
}

impl LimitNode {
    const NODE_NAME: NodeName = "Limit";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        limit: usize,
        offset: Option<usize>,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            limit,
            offset,
            child,
            stats: Arc::new(LimitStats::new(node_id, plan_config.query_id.clone())),
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    fn process_materialized_output(
        self: &Arc<Self>,
        materialized_output: MaterializedOutput,
        limit_state: &mut LimitState,
        task_id_counter: &TaskIDCounter,
    ) -> Vec<SubmittableTask<SwordfishTask>> {
        let node_id = self.node_id();
        let mut downstream_tasks = vec![];
        for next_input in materialized_output.split_into_materialized_outputs() {
            let mut num_rows = next_input.num_rows();

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
                    self.stats.add_active_rows_out(num_rows as u64);
                    make_new_task_from_materialized_outputs(
                        TaskContext::from((&self.context, task_id_counter.next())),
                        vec![next_input],
                        self.config.schema.clone(),
                        &(self.clone() as Arc<dyn PipelineNodeImpl>),
                        move |input| {
                            if skip_num_rows > 0 {
                                LocalPhysicalPlan::limit(
                                    input,
                                    num_rows as u64,
                                    Some(skip_num_rows as u64),
                                    StatsState::NotMaterialized,
                                    LocalNodeContext {
                                        origin_node_id: Some(node_id as usize),
                                        additional: None,
                                    },
                                )
                            } else {
                                input
                            }
                        },
                        None,
                    )
                }
                Ordering::Greater => {
                    let remaining = limit_state.remaining_take();
                    let task = make_new_task_from_materialized_outputs(
                        TaskContext::from((&self.context, task_id_counter.next())),
                        vec![next_input],
                        self.config.schema.clone(),
                        &(self.clone() as Arc<dyn PipelineNodeImpl>),
                        move |input| {
                            LocalPhysicalPlan::limit(
                                input,
                                remaining as u64,
                                Some(skip_num_rows as u64),
                                StatsState::NotMaterialized,
                                LocalNodeContext {
                                    origin_node_id: Some(node_id as usize),
                                    additional: None,
                                },
                            )
                        },
                        None,
                    );
                    limit_state.decrement_take(remaining);
                    self.stats.add_active_rows_out(remaining as u64);
                    task
                }
            };
            downstream_tasks.push(task);
            if limit_state.is_take_done() {
                break;
            }
        }
        downstream_tasks
    }

    async fn limit_execution_loop(
        self: Arc<Self>,
        mut input: SubmittableTaskStream,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        let node_id = self.node_id();
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
                        &(self.clone() as Arc<dyn PipelineNodeImpl>),
                        &move |input| {
                            LocalPhysicalPlan::limit(
                                input,
                                local_limit_per_task as u64,
                                Some(0),
                                StatsState::NotMaterialized,
                                LocalNodeContext {
                                    origin_node_id: Some(node_id as usize),
                                    additional: None,
                                },
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
                    total_num_rows += materialized_output.num_rows();
                    // Process the result and get the next tasks
                    let next_tasks = self.process_materialized_output(
                        materialized_output,
                        &mut limit_state,
                        &task_id_counter,
                    );
                    if next_tasks.is_empty() {
                        // If all rows need to be skipped, send an empty scan task to allow downstream tasks to
                        // continue running, such as aggregate tasks
                        let empty_scan_task = SubmittableTask::new(make_empty_scan_task(
                            TaskContext::from((&self.context, task_id_counter.next())),
                            self.config.schema.clone(),
                            &(self.clone() as Arc<dyn PipelineNodeImpl>),
                        ));

                        if result_tx.send(empty_scan_task).await.is_err() {
                            return Ok(());
                        }
                    } else {
                        // Send the next tasks to the result channel
                        for task in next_tasks {
                            if result_tx.send(task).await.is_err() {
                                return Ok(());
                            }
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
}

impl PipelineNodeImpl for LimitNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        match &self.offset {
            Some(o) => vec![format!("Limit: Num Rows = {}, Offset = {}", self.limit, o)],
            None => vec![format!("Limit: {}", self.limit)],
        }
    }

    fn runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        self.stats.clone()
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_stream = self.child.clone().produce_tasks(plan_context);
        let (result_tx, result_rx) = create_channel(1);

        plan_context.spawn(self.limit_execution_loop(
            input_stream,
            result_tx,
            plan_context.scheduler_handle(),
            plan_context.task_id_counter(),
        ));

        SubmittableTaskStream::from(result_rx)
    }
}
