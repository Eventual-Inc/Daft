use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_metrics::QueryID;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;
use opentelemetry::{global, metrics::Counter};

use super::{DistributedPipelineNode, MaterializedOutput, PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
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
                    self.stats.add_active_rows_out(num_rows as u64);
                    let materialized_outputs = vec![next_input];
                    let in_memory_source_plan = LocalPhysicalPlan::in_memory_scan(
                        self.node_id().to_string(),
                        self.config.schema.clone(),
                        materialized_outputs
                            .iter()
                            .map(|output| output.size_bytes())
                            .sum::<usize>(),
                        StatsState::NotMaterialized,
                        LocalNodeContext {
                            origin_node_id: Some(self.node_id() as usize),
                            additional: None,
                        },
                    );
                    let partition_refs = materialized_outputs
                        .into_iter()
                        .flat_map(|output| output.into_inner().0)
                        .collect::<Vec<_>>();
                    let plan = if skip_num_rows > 0 {
                        LocalPhysicalPlan::limit(
                            in_memory_source_plan,
                            num_rows as u64,
                            Some(skip_num_rows as u64),
                            StatsState::NotMaterialized,
                            LocalNodeContext {
                                origin_node_id: Some(self.node_id() as usize),
                                additional: None,
                            },
                        )
                    } else {
                        in_memory_source_plan
                    };
                    let psets = HashMap::from([(self.node_id().to_string(), partition_refs)]);
                    SwordfishTaskBuilder::new(plan, self.as_ref()).with_psets(psets)
                }
                Ordering::Greater => {
                    let remaining = limit_state.remaining_take();
                    let materialized_outputs = vec![next_input];
                    let in_memory_source_plan = LocalPhysicalPlan::in_memory_scan(
                        self.node_id().to_string(),
                        self.config.schema.clone(),
                        materialized_outputs
                            .iter()
                            .map(|output| output.size_bytes())
                            .sum::<usize>(),
                        StatsState::NotMaterialized,
                        LocalNodeContext {
                            origin_node_id: Some(self.node_id() as usize),
                            additional: None,
                        },
                    );
                    let partition_refs = materialized_outputs
                        .into_iter()
                        .flat_map(|output| output.into_inner().0)
                        .collect::<Vec<_>>();
                    let plan = LocalPhysicalPlan::limit(
                        in_memory_source_plan,
                        remaining as u64,
                        Some(skip_num_rows as u64),
                        StatsState::NotMaterialized,
                        LocalNodeContext {
                            origin_node_id: Some(self.node_id() as usize),
                            additional: None,
                        },
                    );
                    let psets = HashMap::from([(self.node_id().to_string(), partition_refs)]);
                    let task = SwordfishTaskBuilder::new(plan, self.as_ref()).with_psets(psets);
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
        input: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        let mut limit_state = LimitState::new(self.limit, self.offset);

        // Materialize the input stream
        let mut materialized_stream =
            input.materialize(scheduler_handle, self.context.query_idx, task_id_counter);

        // Consume the materialized stream and apply the global limit logic
        let mut send_empty = true;
        while let Some(result) = materialized_stream.next().await {
            let materialized_output = result?;

            // Process the result and get the next tasks
            let next_builders =
                self.process_materialized_output(materialized_output, &mut limit_state);

            // Send the next builders to the result channel
            for builder in next_builders {
                send_empty = false;
                if result_tx.send(builder).await.is_err() {
                    return Ok(());
                }
            }

            if limit_state.is_take_done() {
                break;
            }
        }

        if send_empty {
            let plan = LocalPhysicalPlan::in_memory_scan(
                self.node_id().to_string(),
                self.config.schema.clone(),
                0,
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(self.node_id() as usize),
                    additional: None,
                },
            );
            let empty_scan_builder = SwordfishTaskBuilder::new(plan, self.as_ref());
            let psets = HashMap::from([(self.node_id().to_string(), vec![])]);
            let empty_scan_builder = empty_scan_builder.with_psets(psets);
            let _ = result_tx.send(empty_scan_builder).await;
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
    ) -> TaskBuilderStream {
        let input_stream = self.child.clone().produce_tasks(plan_context);

        // Pipeline the limit operation with limit == self.limit
        let node_id = self.node_id();
        let limit = self.limit + self.offset.unwrap_or(0);
        let local_limit_node = input_stream.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::limit(
                input,
                limit as u64,
                Some(0),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            )
        });

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();
        plan_context.spawn(self.limit_execution_loop(
            local_limit_node,
            result_tx,
            scheduler_handle,
            task_id_counter,
        ));
        TaskBuilderStream::from(result_rx)
    }
}
