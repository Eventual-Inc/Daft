use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_metrics::QueryID;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, stream::FuturesUnordered};
use opentelemetry::{global, metrics::Counter};

use super::{
    DistributedPipelineNode, MaterializedOutput, PipelineNodeImpl, SubmittableTaskStream,
    make_new_task_from_materialized_outputs,
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
    ) -> DaftResult<Vec<SubmittableTask<SwordfishTask>>> {
        let node_id = self.node_id();
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

            // Avoid returning a large number of empty partitoins in sparse scenarios, as the scheduling execution of these empty partitions takes up a lot of time
            if num_rows == 0 {
                continue;
            }

            // If global take is already satisfied, stop producing downstream tasks
            if limit_state.is_take_done() {
                break;
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
                    )?
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
                    )?;
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
        Ok(downstream_tasks)
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
        // Get max tasks to submit in parallel from config
        // - 1: serial execution (original behavior, default)
        // - Other positive values: controlled parallelism
        let mut max_concurrent_tasks = self
            .config
            .execution_config
            .max_limit_tasks_submittable_in_parallel;
        let should_auto_adaptive = max_concurrent_tasks > 1;
        let mut input_exhausted = self.limit == 0;
        let mut emitted_any = false;

        // Keep submitting local limit tasks as long as we have remaining limit or we have input
        while !input_exhausted {
            let mut local_limits = FuturesUnordered::new();
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
                    local_limits.push(future);
                } else {
                    input_exhausted = true;
                    break;
                }
            }
            let num_local_limits = local_limits.len();
            let mut total_num_rows = 0;
            // Process results as they complete (whoever finishes first)
            // FuturesUnordered automatically handles this
            while let Some(maybe_result) = local_limits.next().await {
                if let Ok(Some(materialized_output)) = maybe_result {
                    total_num_rows += materialized_output.num_rows()?;
                    // Process the result and check if we should exit early
                    let downstream_tasks = self.process_materialized_output(
                        materialized_output,
                        &mut limit_state,
                        &task_id_counter,
                    )?;

                    // Send downstream tasks
                    for task in downstream_tasks {
                        if result_tx.send(task).await.is_err() {
                            return Ok(());
                        } else {
                            emitted_any = true;
                        }
                    }

                    // early exit: if we've collected enough rows, cancel remaining tasks
                    if limit_state.is_take_done() {
                        drop(local_limits); // Cancel all remaining futures
                        return Ok(());
                    }
                } else if let Err(e) = maybe_result {
                    return Err(e);
                }
            }

            // Update max_concurrent_tasks based on actual output
            // Only update if we have remaining limit, and we did get some output
            if !limit_state.is_take_done()
                && total_num_rows > 0
                && num_local_limits > 0
                && !should_auto_adaptive
            {
                let rows_per_task = total_num_rows.div_ceil(num_local_limits);
                max_concurrent_tasks = limit_state.remaining_take().div_ceil(rows_per_task);
            }
        }

        // if no tasks were emitted (e.g., all filtered partitions empty), send EmptyScan
        if !emitted_any {
            let task_context = TaskContext::from((&self.context, task_id_counter.next()));
            let empty_plan = LocalPhysicalPlan::empty_scan(
                self.config.schema.clone(),
                LocalNodeContext {
                    origin_node_id: Some(self.node_id() as usize),
                    additional: None,
                },
            );
            let task = SwordfishTask::new(
                task_context,
                empty_plan,
                self.config.execution_config.clone(),
                HashMap::new(),
                crate::scheduling::task::SchedulingStrategy::Spread,
                self.context.to_hashmap(),
            );
            let _ = result_tx.send(SubmittableTask::new(task)).await;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::scheduling::{tests::create_mock_partition_ref, worker::WorkerId};

    fn make_plan_config() -> PlanConfig {
        use common_daft_config::DaftExecutionConfig;
        let cfg = Arc::new(DaftExecutionConfig::default());
        let query_id: common_metrics::QueryID = Arc::from("test_limit");
        PlanConfig::new(0, query_id, cfg)
    }

    fn make_schema() -> SchemaRef {
        use daft_schema::{field::Field, prelude::DataType, schema::Schema};
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]))
    }

    fn make_in_memory_child(
        node_id: NodeID,
        plan_config: &PlanConfig,
        num_parts: usize,
    ) -> DistributedPipelineNode {
        use daft_logical_plan::InMemoryInfo;
        let schema = make_schema();
        let cache_key = "k".to_string();
        let size_bytes = 0usize;
        let total_rows = 0usize;
        let info = InMemoryInfo::new(
            schema.clone(),
            cache_key.clone(),
            None,
            num_parts,
            size_bytes,
            total_rows,
            None,
            None,
        );
        let psets: Arc<std::collections::HashMap<String, Vec<common_partitioning::PartitionRef>>> =
            Arc::new(std::collections::HashMap::from([(
                cache_key.clone(),
                Vec::new(),
            )]));
        super::super::in_memory_source::InMemorySourceNode::new(node_id, plan_config, info, psets)
            .into_node()
    }

    #[test]
    fn test_process_materialized_output_skips_empty_partitions() -> DaftResult<()> {
        // Setup LimitNode and input materialized output with empty partitions
        let plan_config = make_plan_config();
        let child = make_in_memory_child(1, &plan_config, 4);
        let limit_node = Arc::new(LimitNode::new(
            2,
            &plan_config,
            100,
            None,
            make_schema(),
            child,
        ));

        let worker: WorkerId = Arc::from("worker1");
        let partitions = vec![
            create_mock_partition_ref(0, 0),
            create_mock_partition_ref(5, 10),
            create_mock_partition_ref(0, 0),
            create_mock_partition_ref(3, 10),
        ];
        let materialized_output = MaterializedOutput::new(partitions, worker);

        let mut limit_state = LimitState::new(100, None);
        let task_id_counter = crate::plan::TaskIDCounter::new();
        let downstream = limit_node.process_materialized_output(
            materialized_output,
            &mut limit_state,
            &task_id_counter,
        )?;

        // Only non-empty partitions should lead to downstream tasks
        assert_eq!(downstream.len(), 2);
        // Remaining take should decrement by total non-empty rows (5 + 3)
        assert_eq!(limit_state.remaining_take(), 100 - 8);
        Ok(())
    }

    #[test]
    fn test_process_materialized_output_early_exit_when_take_done() -> DaftResult<()> {
        // Setup LimitNode and input materialized output where limit is reached before exhausting inputs
        let plan_config = make_plan_config();
        let child = make_in_memory_child(10, &plan_config, 4);
        let limit_node = Arc::new(LimitNode::new(
            20,
            &plan_config,
            6,
            None,
            make_schema(),
            child,
        ));

        let worker: WorkerId = Arc::from("worker1");
        let partitions = vec![
            create_mock_partition_ref(5, 10),
            create_mock_partition_ref(5, 10),
            create_mock_partition_ref(5, 10),
            create_mock_partition_ref(5, 10),
        ];
        let materialized_output = MaterializedOutput::new(partitions, worker);

        let mut limit_state = LimitState::new(6, None);
        let task_id_counter = crate::plan::TaskIDCounter::new();
        let downstream = limit_node.process_materialized_output(
            materialized_output,
            &mut limit_state,
            &task_id_counter,
        )?;

        // Should only produce tasks up to the limit: first takes 5, second takes 1, then stop
        assert_eq!(downstream.len(), 2);
        assert!(limit_state.is_take_done());
        assert_eq!(limit_state.remaining_take(), 0);
        Ok(())
    }
}
