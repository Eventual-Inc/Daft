use std::{
    collections::HashMap,
    ops::ControlFlow,
    sync::Arc,
    time::{Duration, Instant},
};

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::{
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::StatSnapshotImpl,
};
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads, get_compute_runtime};
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use opentelemetry::metrics::Meter;
use snafu::ResultExt;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput, PipelineExecutionSnafu,
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender, create_channel},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::{BuilderContext, MorselSizeRequirement, NodeName, PipelineNode},
    pipeline_execution::{PipelineEvent, next_event},
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
};

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone)]
pub enum IntermediateOperatorResult {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput {
        input: Arc<MicroPartition>,
        output: Option<Arc<MicroPartition>>,
    },
}

pub(crate) type IntermediateOpExecuteResult<Op> = OperatorOutput<
    DaftResult<(
        <Op as IntermediateOperator>::State,
        IntermediateOperatorResult,
    )>,
>;
pub(crate) trait IntermediateOperator: Send + Sync {
    type State: Send + Sync + Unpin;
    type BatchingStrategy: BatchingStrategy + 'static;
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self>;
    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> Self::State;
    fn make_runtime_stats(&self, meter: &Meter, node_info: &NodeInfo) -> Arc<dyn RuntimeStats> {
        Arc::new(DefaultRuntimeStats::new(meter, node_info))
    }
    /// The maximum number of concurrent workers that can be spawned for this operator.
    /// Each worker will has its own IntermediateOperatorState.
    /// This method should be overridden if the operator needs to limit the number of concurrent workers, i.e. UDFs with resource requests.
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }

    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy>;
}

pub struct IntermediateNode<Op: IntermediateOperator> {
    intermediate_op: Arc<Op>,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

type StateId = usize;

struct ExecutionTaskResult<S> {
    state_id: StateId,
    input_id: InputId,
    state: S,
    result: IntermediateOperatorResult,
    elapsed: Duration,
}

struct InputTracker {
    buffer: RowBasedBuffer,
    in_flight: usize,
    pending_flush: bool,
}

impl InputTracker {
    fn can_flush(&self) -> bool {
        self.pending_flush && self.in_flight == 0 && self.buffer.is_empty()
    }

    fn next_batch_if_ready(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        let batch = self.buffer.next_batch_if_ready()?;
        if batch.is_some() {
            Ok(batch)
        } else if self.pending_flush {
            self.buffer.pop_all()
        } else {
            Ok(None)
        }
    }
}

struct ExecutionContext<Op: IntermediateOperator> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    task_set: OrderingAwareJoinSet<DaftResult<ExecutionTaskResult<Op::State>>>,
    state_pool: HashMap<StateId, Op::State>,
    output_sender: Sender<PipelineMessage>,
    batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    input_trackers: HashMap<InputId, InputTracker>,
    stats_manager: RuntimeStatsManagerHandle,
    node_id: usize,
    node_initialized: bool,
}

// ========== IntermediateNode Implementation ==========

impl<Op: IntermediateOperator + 'static> IntermediateNode<Op> {
    pub(crate) fn new(
        intermediate_op: Arc<Op>,
        child: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = intermediate_op.name().into();
        let info = ctx.next_node_info(
            name,
            intermediate_op.op_type(),
            NodeCategory::Intermediate,
            context,
        );
        let runtime_stats = intermediate_op.make_runtime_stats(&ctx.meter, &info);
        let morsel_size_requirement = intermediate_op
            .morsel_size_requirement()
            .unwrap_or_default();
        Self {
            intermediate_op,
            child,
            runtime_stats,
            plan_stats,
            morsel_size_requirement,
            node_info: Arc::new(info),
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

// ========== Helper Functions ==========

/// Spawn a single task for the given input_id, consuming a state from the pool.
fn spawn_execution_task<Op: IntermediateOperator + 'static>(
    ctx: &mut ExecutionContext<Op>,
    state_id: StateId,
    input_id: InputId,
    op_state: Op::State,
    batch: Arc<MicroPartition>,
) {
    let op = ctx.op.clone();
    let task_spawner = ctx.task_spawner.clone();
    ctx.task_set.spawn(async move {
        let now = Instant::now();
        let (new_op_state, result) = op.execute(batch, op_state, &task_spawner).await??;
        let elapsed = now.elapsed();
        Ok(ExecutionTaskResult {
            state_id,
            input_id,
            state: new_op_state,
            result,
            elapsed,
        })
    });
}

fn try_spawn_tasks<Op: IntermediateOperator + 'static>(
    ctx: &mut ExecutionContext<Op>,
) -> DaftResult<()> {
    let mut input_ids: Vec<InputId> = ctx.input_trackers.keys().copied().collect();
    input_ids.sort_unstable();
    for input_id in input_ids {
        if ctx.task_set.len() >= ctx.op.max_concurrency() || ctx.state_pool.is_empty() {
            break;
        }
        try_spawn_tasks_for_input(ctx, input_id)?;
    }
    Ok(())
}

fn try_spawn_tasks_for_input<Op: IntermediateOperator + 'static>(
    ctx: &mut ExecutionContext<Op>,
    input_id: InputId,
) -> DaftResult<()> {
    while !ctx.state_pool.is_empty() {
        let input = {
            let tracker = ctx
                .input_trackers
                .get_mut(&input_id)
                .expect("Input should be present");
            let batch = tracker.next_batch_if_ready()?;
            let Some(batch) = batch else {
                break;
            };
            tracker.in_flight += 1;
            batch
        };
        let state_id = *ctx
            .state_pool
            .keys()
            .next()
            .expect("State pool should have states when non-empty");
        let op_state = ctx
            .state_pool
            .remove(&state_id)
            .expect("State pool should have states when non-empty");
        spawn_execution_task(ctx, state_id, input_id, op_state, input);
    }
    Ok(())
}

async fn try_flush_input<Op: IntermediateOperator + 'static>(
    ctx: &mut ExecutionContext<Op>,
    input_id: InputId,
) -> DaftResult<ControlFlow<(), ()>> {
    let input_state = ctx
        .input_trackers
        .get_mut(&input_id)
        .expect("Input should be present");
    if input_state.can_flush() {
        ctx.input_trackers.remove(&input_id);
        if ctx
            .output_sender
            .send(PipelineMessage::Flush(input_id))
            .await
            .is_err()
        {
            return Ok(ControlFlow::Break(()));
        }
        return Ok(ControlFlow::Continue(()));
    }
    Ok(ControlFlow::Continue(()))
}

async fn handle_task_completion<Op: IntermediateOperator + 'static>(
    ctx: &mut ExecutionContext<Op>,
    task_result: ExecutionTaskResult<Op::State>,
) -> DaftResult<ControlFlow<(), ()>> {
    let ExecutionTaskResult {
        state_id,
        input_id,
        state,
        result,
        elapsed,
    } = task_result;

    ctx.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

    if let Some(mp) = result.output() {
        ctx.runtime_stats.add_rows_out(mp.len() as u64);
        ctx.batch_manager
            .record_execution_stats(ctx.runtime_stats.as_ref(), mp.len(), elapsed);
        if ctx
            .output_sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition: mp.clone(),
            })
            .await
            .is_err()
        {
            return Ok(ControlFlow::Break(()));
        }
    }

    let new_requirements = ctx.batch_manager.calculate_batch_size();
    for input in ctx.input_trackers.values_mut() {
        input.buffer.update_bounds(new_requirements);
    }

    match result {
        IntermediateOperatorResult::NeedMoreInput(_) => {
            ctx.input_trackers.get_mut(&input_id).unwrap().in_flight -= 1;
            ctx.state_pool.insert(state_id, state);
            try_spawn_tasks(ctx)?;
            try_flush_input(ctx, input_id).await
        }
        IntermediateOperatorResult::HasMoreOutput { input, .. } => {
            spawn_execution_task(ctx, state_id, input_id, state, input);
            Ok(ControlFlow::Continue(()))
        }
    }
}

fn handle_morsel<Op: IntermediateOperator + 'static>(
    ctx: &mut ExecutionContext<Op>,
    input_id: InputId,
    partition: Arc<MicroPartition>,
) -> DaftResult<()> {
    if !ctx.node_initialized {
        ctx.stats_manager.activate_node(ctx.node_id);
        ctx.node_initialized = true;
    }
    ctx.runtime_stats.add_rows_in(partition.len() as u64);
    let (lower, upper) = ctx.batch_manager.calculate_batch_size().values();
    let input = ctx
        .input_trackers
        .entry(input_id)
        .or_insert_with(|| InputTracker {
            buffer: RowBasedBuffer::new(lower, upper),
            in_flight: 0,
            pending_flush: false,
        });
    input.buffer.push(partition);
    try_spawn_tasks(ctx)
}

async fn handle_flush<Op: IntermediateOperator + 'static>(
    ctx: &mut ExecutionContext<Op>,
    input_id: InputId,
) -> DaftResult<ControlFlow<(), ()>> {
    let Some(input) = ctx.input_trackers.get_mut(&input_id) else {
        return Ok(
            if ctx
                .output_sender
                .send(PipelineMessage::Flush(input_id))
                .await
                .is_err()
            {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            },
        );
    };
    input.pending_flush = true;
    try_spawn_tasks(ctx)?;
    try_flush_input(ctx, input_id).await
}

async fn handle_input_closed<Op: IntermediateOperator + 'static>(
    ctx: &mut ExecutionContext<Op>,
) -> DaftResult<ControlFlow<(), ()>> {
    for input_state in ctx.input_trackers.values_mut() {
        input_state.pending_flush = true;
    }
    try_spawn_tasks(ctx)?;
    let mut input_ids: Vec<InputId> = ctx.input_trackers.keys().copied().collect();
    input_ids.sort_unstable();
    for input_id in input_ids {
        if try_flush_input(ctx, input_id).await?.is_break() {
            return Ok(ControlFlow::Break(()));
        }
    }
    Ok(ControlFlow::Continue(()))
}

async fn process_input<Op: IntermediateOperator + 'static>(
    ctx: &mut ExecutionContext<Op>,
    receiver: &mut Receiver<PipelineMessage>,
) -> DaftResult<()> {
    let mut input_closed = false;

    while !input_closed || !ctx.task_set.is_empty() || !ctx.input_trackers.is_empty() {
        let event = next_event(
            &mut ctx.task_set,
            ctx.op.max_concurrency(),
            receiver,
            &mut input_closed,
        )
        .await?;
        let cf = match event {
            PipelineEvent::TaskCompleted(task_result) => {
                handle_task_completion(ctx, task_result).await?
            }
            PipelineEvent::Morsel {
                input_id,
                partition,
            } => {
                handle_morsel(ctx, input_id, partition)?;
                ControlFlow::Continue(())
            }
            PipelineEvent::Flush(input_id) => handle_flush(ctx, input_id).await?,
            PipelineEvent::InputClosed => handle_input_closed(ctx).await?,
        };
        if cf.is_break() {
            return Ok(());
        }
    }
    Ok(())
}

impl IntermediateOperatorResult {
    /// Get the output partition if present.
    pub(crate) fn output(&self) -> Option<&Arc<MicroPartition>> {
        match self {
            Self::NeedMoreInput(mp) => mp.as_ref(),
            Self::HasMoreOutput { output, .. } => output.as_ref(),
        }
    }
}

impl<Op: IntermediateOperator + 'static> TreeDisplay for IntermediateNode<Op> {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        use common_display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.intermediate_op.name()).unwrap();
            }
            level => {
                let multiline_display = self.intermediate_op.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
                writeln!(display, "Batch Size = {}", self.morsel_size_requirement).unwrap();
                if matches!(level, DisplayLevel::Verbose) {
                    writeln!(display).unwrap();
                    let rt_result = self.runtime_stats.snapshot();
                    for (name, value) in rt_result.to_stats() {
                        writeln!(display, "{} = {}", name.as_ref().capitalize(), value).unwrap();
                    }
                }
            }
        }
        display
    }

    fn repr_json(&self) -> serde_json::Value {
        let children: Vec<serde_json::Value> = self
            .get_children()
            .iter()
            .map(|child| child.repr_json())
            .collect();

        serde_json::json!({
            "id": self.node_id(),
            "category": "Intermediate",
            "type": self.intermediate_op.op_type().to_string(),
            "name": self.name(),
            "children": children,
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

impl<Op: IntermediateOperator + 'static> PipelineNode for IntermediateNode<Op> {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        vec![&self.child]
    }

    fn name(&self) -> Arc<str> {
        self.node_info.name.clone()
    }

    fn propagate_morsel_size_requirement(
        &mut self,
        downstream_requirement: MorselSizeRequirement,
        default_requirement: MorselSizeRequirement,
    ) {
        let operator_morsel_size_requirement = self.intermediate_op.morsel_size_requirement();
        let combined_morsel_size_requirement = MorselSizeRequirement::combine_requirements(
            operator_morsel_size_requirement,
            downstream_requirement,
        );
        self.morsel_size_requirement = combined_morsel_size_requirement;
        self.child.propagate_morsel_size_requirement(
            combined_morsel_size_requirement,
            default_requirement,
        );
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        // 1. Start children and wrap receivers
        let mut child_result_receiver = self.child.start(maintain_order, runtime_handle)?;
        // 2. Setup
        let op = self.intermediate_op.clone();
        let (destination_sender, destination_receiver) = create_channel(1);

        // 3. Create task spawner
        let compute_runtime = get_compute_runtime();
        let task_spawner = ExecutionTaskSpawner::new(
            compute_runtime,
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("IntermediateOp::execute"),
        );

        // 4. Spawn process_input task
        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();
        let runtime_stats = self.runtime_stats.clone();
        runtime_handle.spawn(
            async move {
                let batch_manager = Arc::new(BatchManager::new(op.batching_strategy().context(
                    PipelineExecutionSnafu {
                        node_name: op.name().to_string(),
                    },
                )?));
                let state_pool = (0..op.max_concurrency())
                    .map(|i| (i, op.make_state()))
                    .collect();
                let mut ctx = ExecutionContext {
                    op,
                    task_spawner,
                    task_set: OrderingAwareJoinSet::new(maintain_order),
                    state_pool,
                    output_sender: destination_sender,
                    batch_manager,
                    runtime_stats,
                    input_trackers: HashMap::new(),
                    stats_manager: stats_manager.clone(),
                    node_id,
                    node_initialized: false,
                };

                process_input(&mut ctx, &mut child_result_receiver).await?;

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &self.name(),
        );

        Ok(destination_receiver)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
    fn node_id(&self) -> usize {
        self.node_info.id
    }

    fn plan_id(&self) -> Arc<str> {
        Arc::from(self.node_info.context.get("plan_id").unwrap().clone())
    }

    fn node_info(&self) -> Arc<NodeInfo> {
        self.node_info.clone()
    }

    fn runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        self.runtime_stats.clone()
    }
}
