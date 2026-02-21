use std::{
    collections::{HashMap, hash_map::Entry},
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
use common_runtime::{JoinSet, OrderingAwareJoinSet, get_compute_pool_num_threads, get_compute_runtime};
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use opentelemetry::metrics::Meter;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender, create_channel},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::{BuilderContext, MorselSizeRequirement, NodeName, PipelineNode},
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
};

pub enum StreamingSinkOutput {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput {
        input: Arc<MicroPartition>,
        output: Option<Arc<MicroPartition>>,
    },
    Finished(Option<Arc<MicroPartition>>),
}

pub enum StreamingSinkFinalizeOutput<Op: StreamingSink> {
    HasMoreOutput {
        states: Vec<Op::State>,
        output: Option<Arc<MicroPartition>>,
    },
    Finished(Option<Arc<MicroPartition>>),
}

pub(crate) type StreamingSinkExecuteResult<Op> =
    OperatorOutput<DaftResult<(<Op as StreamingSink>::State, StreamingSinkOutput)>>;
pub(crate) type StreamingSinkFinalizeResult<Op> =
    OperatorOutput<DaftResult<StreamingSinkFinalizeOutput<Op>>>;
pub(crate) trait StreamingSink: Send + Sync {
    type State: Send + Sync + Unpin;
    type BatchingStrategy: BatchingStrategy + 'static;

    /// Execute the StreamingSink operator on the morsel of input data,
    /// received from the child,
    /// with the given state.
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self>;

    /// Finalize the StreamingSink operator, with the given states from each worker.
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self>
    where
        Self: Sized;

    /// The name of the StreamingSink operator. Used for display purposes.
    fn name(&self) -> NodeName;

    /// The type of the StreamingSink operator.
    fn op_type(&self) -> NodeType;

    fn multiline_display(&self) -> Vec<String>;

    /// Create a new worker-local state for this StreamingSink.
    fn make_state(&self) -> DaftResult<Self::State>;

    /// Create a new RuntimeStats for this StreamingSink.
    fn make_runtime_stats(&self, meter: &Meter, node_info: &NodeInfo) -> Arc<dyn RuntimeStats> {
        Arc::new(DefaultRuntimeStats::new(meter, node_info))
    }

    /// The maximum number of concurrent workers that can be spawned for this sink.
    /// Each worker will has its own StreamingSinkState.
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }
    fn batching_strategy(&self) -> Self::BatchingStrategy;
}

pub struct StreamingSinkNode<Op: StreamingSink> {
    op: Arc<Op>,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
    morsel_size_requirement: MorselSizeRequirement,
}

type StateId = usize;

struct ExecutionTaskResult<S> {
    state_id: StateId,
    state: S,
    output: StreamingSinkOutput,
    elapsed: Duration,
}

struct ExecutionContext<Op: StreamingSink> {
    input_id: InputId,
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    finalize_spawner: ExecutionTaskSpawner,
    task_set: OrderingAwareJoinSet<DaftResult<ExecutionTaskResult<Op::State>>>,
    state_pool: HashMap<StateId, Op::State>,
    output_sender: Sender<PipelineMessage>,
    batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    stats_manager: RuntimeStatsManagerHandle,
}

impl<Op: StreamingSink + 'static> StreamingSinkNode<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        child: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = op.name().into();
        let node_info =
            ctx.next_node_info(name, op.op_type(), NodeCategory::StreamingSink, context);
        let runtime_stats = op.make_runtime_stats(&ctx.meter, &node_info);

        let morsel_size_requirement = op.morsel_size_requirement().unwrap_or_default();
        Self {
            op,
            child,
            runtime_stats,
            plan_stats,
            node_info: Arc::new(node_info),
            morsel_size_requirement,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    // ========== Helper Functions ==========

    fn spawn_execution_task(
        ctx: &mut ExecutionContext<Op>,
        input: Arc<MicroPartition>,
        state: Op::State,
        state_id: StateId,
    ) {
        spawn_execution_task_impl(ctx, input, state, state_id);
    }

    fn spawn_ready_batches(
        buffer: &mut RowBasedBuffer,
        ctx: &mut ExecutionContext<Op>,
    ) -> DaftResult<()> {
        spawn_ready_batches_impl(buffer, ctx)
    }

    async fn handle_task_completion(
        result: ExecutionTaskResult<Op::State>,
        ctx: &mut ExecutionContext<Op>,
    ) -> DaftResult<ControlFlow<(), ()>> {
        handle_task_completion_impl(result, ctx).await
    }

    async fn process_input(
        node_id: usize,
        receiver: Receiver<PipelineMessage>,
        ctx: &mut ExecutionContext<Op>,
    ) -> DaftResult<ControlFlow<(), ()>> {
        process_input_impl(node_id, receiver, ctx).await
    }
}

fn spawn_execution_task_impl<Op: StreamingSink + 'static>(
    ctx: &mut ExecutionContext<Op>,
    input: Arc<MicroPartition>,
    state: Op::State,
    state_id: StateId,
) {
    let op = ctx.op.clone();
    let task_spawner = ctx.task_spawner.clone();
    ctx.task_set.spawn(async move {
        let now = Instant::now();
        let (new_state, result) = op.execute(input, state, &task_spawner).await??;
        let elapsed = now.elapsed();

        Ok(ExecutionTaskResult {
            state_id,
            state: new_state,
            output: result,
            elapsed,
        })
    });
}

fn spawn_ready_batches_impl<Op: StreamingSink + 'static>(
    buffer: &mut RowBasedBuffer,
    ctx: &mut ExecutionContext<Op>,
) -> DaftResult<()> {
    while !ctx.state_pool.is_empty() && let Some(batch) = buffer.next_batch_if_ready()? {
        let state_id = *ctx
            .state_pool
            .keys()
            .next()
            .expect("State pool should have states when it is not empty");
        let state = ctx
            .state_pool
            .remove(&state_id)
            .expect("State pool should have states when it is not empty");

        spawn_execution_task_impl(ctx, batch, state, state_id);
    }
    Ok(())
}

async fn handle_task_completion_impl<Op: StreamingSink + 'static>(
    result: ExecutionTaskResult<Op::State>,
    ctx: &mut ExecutionContext<Op>,
) -> DaftResult<ControlFlow<(), ()>> {
    match result.output {
        StreamingSinkOutput::NeedMoreInput(mp) => {
            ctx.batch_manager.record_execution_stats(
                ctx.runtime_stats.as_ref(),
                mp.as_ref().map(|mp| mp.len()).unwrap_or(0),
                result.elapsed,
            );
            if let Some(mp) = mp {
                ctx.runtime_stats.add_rows_out(mp.len() as u64);
                if ctx
                    .output_sender
                    .send(PipelineMessage::Morsel {
                        input_id: ctx.input_id,
                        partition: mp,
                    })
                    .await
                    .is_err()
                {
                    return Ok(ControlFlow::Break(()));
                }
            }
            ctx.state_pool.insert(result.state_id, result.state);
        }
        StreamingSinkOutput::HasMoreOutput { input, output } => {
            ctx.batch_manager.record_execution_stats(
                ctx.runtime_stats.as_ref(),
                output.as_ref().map(|mp| mp.len()).unwrap_or(0),
                result.elapsed,
            );
            if let Some(mp) = output {
                ctx.runtime_stats.add_rows_out(mp.len() as u64);
                if ctx
                    .output_sender
                    .send(PipelineMessage::Morsel {
                        input_id: ctx.input_id,
                        partition: mp,
                    })
                    .await
                    .is_err()
                {
                    return Ok(ControlFlow::Break(()));
                }
            }
            spawn_execution_task_impl(ctx, input, result.state, result.state_id);
        }
        StreamingSinkOutput::Finished(mp) => {
            ctx.batch_manager.record_execution_stats(
                ctx.runtime_stats.as_ref(),
                mp.as_ref().map(|mp| mp.len()).unwrap_or(0),
                result.elapsed,
            );
            if let Some(mp) = mp {
                ctx.runtime_stats.add_rows_out(mp.len() as u64);
                let _ = ctx
                    .output_sender
                    .send(PipelineMessage::Morsel {
                        input_id: ctx.input_id,
                        partition: mp,
                    })
                    .await;
            }
            ctx.state_pool.insert(result.state_id, result.state);
            return Ok(ControlFlow::Break(()));
        }
    }
    Ok(ControlFlow::Continue(()))
}

async fn process_input_impl<Op: StreamingSink + 'static>(
    node_id: usize,
    mut receiver: Receiver<PipelineMessage>,
    ctx: &mut ExecutionContext<Op>,
) -> DaftResult<ControlFlow<(), ()>> {
    let (lower, upper) = ctx.batch_manager.calculate_batch_size().values();
    let mut buffer = RowBasedBuffer::new(lower, upper);
    let mut input_closed = false;
    let mut node_initialized = false;
    let mut finished = false;

    while !finished && (!input_closed || !ctx.task_set.is_empty()) {
        tokio::select! {
            biased;

            Some(join_result) = ctx.task_set.join_next(), if !ctx.task_set.is_empty() => {
                let result = join_result??;
                if handle_task_completion_impl(result, ctx).await?.is_break() {
                    return Ok(ControlFlow::Break(()));
                }

                let new_requirements = ctx.batch_manager.calculate_batch_size();
                buffer.update_bounds(new_requirements);

                spawn_ready_batches_impl(&mut buffer, ctx)?;
            }

            msg = receiver.recv(), if !ctx.state_pool.is_empty() && !input_closed => {
                match msg {
                    Some(PipelineMessage::Morsel { partition, .. }) => {
                        if !node_initialized {
                            ctx.stats_manager.activate_node(node_id);
                            node_initialized = true;
                        }
                        ctx.runtime_stats.add_rows_in(partition.len() as u64);
                        buffer.push(partition);
                        spawn_ready_batches_impl(&mut buffer, ctx)?;
                    }
                    Some(PipelineMessage::Flush(_)) | None => {
                        input_closed = true;
                    }
                }
            }
        }
    }

    // Drain remaining buffer
    if !finished {
        if let Some(mut partition) = buffer.pop_all()? {
            let mut state = ctx
                .state_pool
                .drain()
                .next()
                .map(|(_, s)| s)
                .expect("state_pool non-empty");
            loop {
                let now = Instant::now();
                let (new_state, result) = ctx
                    .op
                    .execute(partition, state, &ctx.task_spawner)
                    .await??;
                let elapsed = now.elapsed();
                ctx.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

                if let Some(mp) = result.output() {
                    ctx.runtime_stats.add_rows_out(mp.len() as u64);
                    ctx.batch_manager.record_execution_stats(
                        ctx.runtime_stats.as_ref(),
                        mp.len(),
                        elapsed,
                    );
                    if ctx
                        .output_sender
                        .send(PipelineMessage::Morsel {
                            input_id: ctx.input_id,
                            partition: mp.clone(),
                        })
                        .await
                        .is_err()
                    {
                        return Ok(ControlFlow::Break(()));
                    }
                }

                let new_requirements = ctx.batch_manager.calculate_batch_size();
                buffer.update_bounds(new_requirements);

                match result {
                    StreamingSinkOutput::NeedMoreInput(_) => {
                        ctx.state_pool.insert(0, new_state);
                        break;
                    }
                    StreamingSinkOutput::HasMoreOutput { input, .. } => {
                        partition = input;
                        state = new_state;
                    }
                    StreamingSinkOutput::Finished(_) => {
                        finished = true;
                        break;
                    }
                }
            }
        }
    }

    // Finalize if not already finished
    if !finished {
        let mut states: Vec<Op::State> = ctx.state_pool.drain().map(|(_, s)| s).collect();
        loop {
            match ctx.op.finalize(states, &ctx.finalize_spawner).await?? {
                StreamingSinkFinalizeOutput::HasMoreOutput {
                    states: new_states,
                    output,
                } => {
                    if let Some(mp) = output {
                        ctx.runtime_stats.add_rows_out(mp.len() as u64);
                        if ctx
                            .output_sender
                            .send(PipelineMessage::Morsel {
                                input_id: ctx.input_id,
                                partition: mp,
                            })
                            .await
                            .is_err()
                        {
                            return Ok(ControlFlow::Break(()));
                        }
                    }
                    states = new_states;
                }
                StreamingSinkFinalizeOutput::Finished(output) => {
                    if let Some(mp) = output {
                        ctx.runtime_stats.add_rows_out(mp.len() as u64);
                        if ctx
                            .output_sender
                            .send(PipelineMessage::Morsel {
                                input_id: ctx.input_id,
                                partition: mp,
                            })
                            .await
                            .is_err()
                        {
                            return Ok(ControlFlow::Break(()));
                        }
                    }
                    break;
                }
            }
        }
    }

    if ctx
        .output_sender
        .send(PipelineMessage::Flush(ctx.input_id))
        .await
        .is_err()
    {
        return Ok(ControlFlow::Break(()));
    }
    Ok(ControlFlow::Continue(()))
}

impl StreamingSinkOutput {
    pub(crate) fn output(&self) -> Option<&Arc<MicroPartition>> {
        match self {
            Self::NeedMoreInput(mp) => mp.as_ref(),
            Self::HasMoreOutput { output, .. } => output.as_ref(),
            Self::Finished(mp) => mp.as_ref(),
        }
    }
}

impl<Op: StreamingSink + 'static> TreeDisplay for StreamingSinkNode<Op> {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        use common_display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.op.name()).unwrap();
            }
            level => {
                let multiline_display = self.op.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
                writeln!(display, "Batch Size = {}", self.morsel_size_requirement).unwrap();
                if matches!(level, DisplayLevel::Verbose) {
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
            "category": "StreamingSink",
            "type": self.op.op_type().to_string(),
            "name": self.name(),
            "children": children,
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

impl<Op: StreamingSink + 'static> PipelineNode for StreamingSinkNode<Op> {
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
        _default_morsel_size: MorselSizeRequirement,
    ) {
        let operator_morsel_size_requirement = self.op.morsel_size_requirement();
        let combined_morsel_size_requirement = MorselSizeRequirement::combine_requirements(
            operator_morsel_size_requirement,
            downstream_requirement,
        );
        self.morsel_size_requirement = combined_morsel_size_requirement;
        self.child.propagate_morsel_size_requirement(
            combined_morsel_size_requirement,
            _default_morsel_size,
        );
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let mut child_results_receiver = self.child.start(maintain_order, runtime_handle)?;
        let (destination_sender, destination_receiver) = create_channel(1);

        let task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("StreamingSink::Execute"),
        );
        let finalize_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("StreamingSink::Finalize"),
        );

        let batch_manager = Arc::new(BatchManager::new(self.op.batching_strategy()));
        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let node_id = self.node_id();
        let stats_manager = runtime_handle.stats_manager();

        runtime_handle.spawn(
            async move {
                let mut per_input_senders: HashMap<InputId, Sender<PipelineMessage>> =
                    HashMap::new();
                let mut processor_set: JoinSet<DaftResult<ControlFlow<(), ()>>> = JoinSet::new();
                let mut node_initialized = false;
                let mut input_closed = false;

                while !input_closed || !processor_set.is_empty() {
                    tokio::select! {
                        msg = child_results_receiver.recv(), if !input_closed => {
                            let Some(msg) = msg else {
                                input_closed = true;
                                per_input_senders.clear();
                                continue;
                            };

                            if !node_initialized {
                                stats_manager.activate_node(node_id);
                                node_initialized = true;
                            }

                            let input_id = match &msg {
                                PipelineMessage::Morsel { input_id, .. } => *input_id,
                                PipelineMessage::Flush(input_id) => *input_id,
                            };

                            if let Entry::Vacant(e) = per_input_senders.entry(input_id) {
                                let (tx, rx) = create_channel(1);
                                e.insert(tx);

                                let op = op.clone();
                                let task_spawner = task_spawner.clone();
                                let finalize_spawner = finalize_spawner.clone();
                                let runtime_stats = runtime_stats.clone();
                                let batch_manager = batch_manager.clone();
                                let stats_manager = stats_manager.clone();
                                let state_pool = (0..op.max_concurrency())
                                    .map(|i| op.make_state().map(|s| (i, s)))
                                    .collect::<DaftResult<HashMap<_, _>>>()?;
                                let mut ctx = ExecutionContext {
                                    input_id,
                                    op: op.clone(),
                                    task_spawner: task_spawner.clone(),
                                    finalize_spawner: finalize_spawner.clone(),
                                    task_set: OrderingAwareJoinSet::new(maintain_order),
                                    state_pool,
                                    output_sender: destination_sender.clone(),
                                    batch_manager,
                                    runtime_stats,
                                    stats_manager,
                                };
                                processor_set.spawn(async move {
                                    process_input_impl::<Op>(node_id, rx, &mut ctx).await
                                });
                            }

                            let is_flush = matches!(&msg, PipelineMessage::Flush(_));
                            if per_input_senders[&input_id].send(msg).await.is_err() {
                                // Processor died — error will surface from join below
                            }
                            if is_flush {
                                per_input_senders.remove(&input_id);
                            }
                        }
                        Some(result) = processor_set.join_next(), if !processor_set.is_empty() => {
                            if result??.is_break() {
                                break;
                            }
                        }
                    }
                }

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
