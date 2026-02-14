use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use common_error::DaftResult;
use common_metrics::{
    CPU_US_KEY, Counter, ROWS_IN_KEY, ROWS_OUT_KEY, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::DefaultSnapshot,
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;
use opentelemetry::{KeyValue, metrics::Meter};

use super::{DistributedPipelineNode, MaterializedOutput, PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::{RuntimeStats, stats::RuntimeStatsRef},
    utils::channel::{Sender, create_channel},
};

const FIRST_LIMIT_STAGE: &str = "0";
const SECOND_LIMIT_STAGE: &str = "1";

pub struct LimitStats {
    cpu_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    node_kv: Vec<KeyValue>,
}

impl LimitStats {
    pub fn new(meter: &Meter, node_id: NodeID) -> Self {
        let node_kv = vec![KeyValue::new("node_id", node_id.to_string())];
        Self {
            cpu_us: Counter::new(meter, CPU_US_KEY, None),
            rows_in: Counter::new(meter, ROWS_IN_KEY, None),
            rows_out: Counter::new(meter, ROWS_OUT_KEY, None),
            node_kv,
        }
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.add(cpu_us, self.node_kv.as_slice());
    }
    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }
    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }
}

impl RuntimeStats for LimitStats {
    fn handle_worker_node_stats(&self, node_info: &NodeInfo, snapshot: &StatSnapshot) {
        match snapshot {
            StatSnapshot::Default(snapshot) => {
                self.add_cpu_us(snapshot.cpu_us);
                if let Some(stage) = node_info.context.get("stage") {
                    // The first limit is used for pruning, the second limit is for the final output
                    if stage == FIRST_LIMIT_STAGE {
                        self.add_rows_in(snapshot.rows_in);
                    } else if stage == SECOND_LIMIT_STAGE {
                        self.add_rows_out(snapshot.rows_out);
                    }
                }
            }
            StatSnapshot::Source(snapshot) => {
                self.add_cpu_us(snapshot.cpu_us);
                if let Some(stage) = node_info.context.get("stage")
                    && stage == SECOND_LIMIT_STAGE
                {
                    self.add_rows_out(snapshot.rows_out);
                }
            }
            _ => {} // Limit don't receive stats from other Swordfish nodes
        }
    }

    fn export_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.cpu_us.load(std::sync::atomic::Ordering::SeqCst),
            rows_in: self.rows_in.load(std::sync::atomic::Ordering::SeqCst),
            rows_out: self.rows_out.load(std::sync::atomic::Ordering::SeqCst),
        })
    }
}

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
    child: DistributedPipelineNode,
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
            NodeType::Limit,
            NodeCategory::StreamingSink,
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
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    fn process_materialized_output(
        self: &Arc<Self>,
        materialized_output: MaterializedOutput,
        limit_state: &mut LimitState,
    ) -> Vec<SwordfishTaskBuilder> {
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
                    let materialized_outputs = vec![next_input];

                    let (plan, psets) = if skip_num_rows > 0 {
                        let (in_memory_scan, psets) =
                            MaterializedOutput::into_in_memory_scan_with_psets(
                                materialized_outputs,
                                self.config.schema.clone(),
                                self.node_id(),
                            );

                        (
                            LocalPhysicalPlan::limit(
                                in_memory_scan,
                                num_rows as u64,
                                Some(skip_num_rows as u64),
                                StatsState::NotMaterialized,
                                LocalNodeContext {
                                    origin_node_id: Some(self.node_id() as usize),
                                    additional: Some(HashMap::from([(
                                        "stage".to_string(),
                                        SECOND_LIMIT_STAGE.to_string(),
                                    )])),
                                },
                            ),
                            psets,
                        )
                    } else {
                        let (in_memory_scan, psets) =
                            MaterializedOutput::into_in_memory_scan_with_psets_and_context(
                                materialized_outputs,
                                self.config.schema.clone(),
                                self.node_id(),
                                Some(HashMap::from([(
                                    "stage".to_string(),
                                    SECOND_LIMIT_STAGE.to_string(),
                                )])),
                            );

                        (in_memory_scan, psets)
                    };
                    SwordfishTaskBuilder::new(plan, self.as_ref()).with_psets(self.node_id(), psets)
                }
                Ordering::Greater => {
                    let remaining = limit_state.remaining_take();
                    let materialized_outputs = vec![next_input];
                    let (in_memory_scan, psets) =
                        MaterializedOutput::into_in_memory_scan_with_psets(
                            materialized_outputs,
                            self.config.schema.clone(),
                            self.node_id(),
                        );
                    let plan = LocalPhysicalPlan::limit(
                        in_memory_scan,
                        remaining as u64,
                        Some(skip_num_rows as u64),
                        StatsState::NotMaterialized,
                        LocalNodeContext {
                            origin_node_id: Some(self.node_id() as usize),
                            additional: Some(HashMap::from([(
                                "stage".to_string(),
                                SECOND_LIMIT_STAGE.to_string(),
                            )])),
                        },
                    );
                    let task = SwordfishTaskBuilder::new(plan, self.as_ref())
                        .with_psets(self.node_id(), psets);
                    limit_state.decrement_take(remaining);
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
        mut input: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
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
                if let Some(builder) = input.next().await {
                    let builder_with_limit = builder.map_plan(self.as_ref(), move |input| {
                        LocalPhysicalPlan::limit(
                            input,
                            local_limit_per_task as u64,
                            Some(0),
                            StatsState::NotMaterialized,
                            LocalNodeContext {
                                origin_node_id: Some(node_id as usize),
                                additional: Some(HashMap::from([(
                                    "stage".to_string(),
                                    FIRST_LIMIT_STAGE.to_string(),
                                )])),
                            },
                        )
                    });
                    let submittable =
                        builder_with_limit.build(self.context.query_idx, &task_id_counter);
                    let future = submittable.submit(&scheduler_handle)?;
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
                    let next_tasks =
                        self.process_materialized_output(materialized_output, &mut limit_state);
                    if next_tasks.is_empty() {
                        // If all rows need to be skipped, send an empty scan task to allow downstream tasks to
                        // continue running, such as aggregate tasks
                        let empty_plan = LocalPhysicalPlan::empty_scan(
                            self.config.schema.clone(),
                            LocalNodeContext {
                                origin_node_id: Some(self.node_id() as usize),
                                additional: None,
                            },
                        );
                        let empty_scan_builder =
                            SwordfishTaskBuilder::new(empty_plan, self.as_ref());
                        if result_tx.send(empty_scan_builder).await.is_err() {
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

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(LimitStats::new(meter, self.node_id()))
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        match &self.offset {
            Some(o) => vec![format!("Limit: Num Rows = {}, Offset = {}", self.limit, o)],
            None => vec![format!("Limit: {}", self.limit)],
        }
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_stream = self.child.clone().produce_tasks(plan_context);
        let (result_tx, result_rx) = create_channel(1);

        plan_context.spawn(self.limit_execution_loop(
            input_stream,
            result_tx,
            plan_context.scheduler_handle(),
            plan_context.task_id_counter(),
        ));
        TaskBuilderStream::from(result_rx)
    }
}
