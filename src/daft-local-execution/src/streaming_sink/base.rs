use std::{
    collections::{HashMap, hash_map::Entry},
    ops::ControlFlow,
    sync::Arc,
    time::{Duration, Instant},
};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeInfo, NodeType},
};
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads, get_compute_runtime};
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    batch_manager::BatchManager,
    channel::{Receiver, Sender, create_channel},
    dynamic_batching::BatchingStrategy,
    input_cancel::InputCancelRegistry,
    pipeline::{
        BuilderContext, InputId, MorselSizeRequirement, NodeName, PipelineEvent, PipelineMessage,
        PipelineNode, next_event,
    },
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
};

pub enum StreamingSinkOutput {
    NeedMoreInput(Option<MicroPartition>),
    /// The operator has all the data it will ever need for *this* `input_id`,
    /// but the node must keep running for the others.
    ///
    /// Producers feeding this input are asked to stop (see
    /// [`InputCancelRegistry`]) and anything already buffered for it is
    /// dropped, yet the input still completes through its own
    /// `PipelineMessage::Flush` exactly as an uncancelled one would. Operators
    /// returning this must opt in via [`StreamingSink::cancels_inputs`].
    ///
    /// This is separate from `Finished` because `Finished` tears down the whole
    /// node — correct for a single-task pipeline, fatal for a flotilla worker
    /// pipeline shared by every task with the same plan fingerprint.
    ///
    /// Truncating a subtree is safe even when it contains an operator that must
    /// see all of its input before it may emit — an aggregation, a sort, a
    /// top-N. Those are all `BlockingSink`s, and a `BlockingSink` emits nothing
    /// until it has drained its input, so no row can reach this operator (and
    /// hence nothing can be satisfied) before every such operator below it has
    /// already consumed everything it needed.
    InputSatisfied(Option<MicroPartition>),
    /// The operator is done with the entire node; the pipeline below it is torn
    /// down. Only safe when the node serves exactly one input.
    Finished(Option<MicroPartition>),
}

/// What [`ExecutionContext`] should do after an operator returned a
/// [`StreamingSinkOutput`].
#[derive(Copy, Clone, PartialEq, Eq)]
enum Disposition {
    KeepGoing,
    InputSatisfied,
    Finished,
}

impl StreamingSinkOutput {
    fn split(self) -> (Option<MicroPartition>, Disposition) {
        match self {
            Self::NeedMoreInput(mp) => (mp, Disposition::KeepGoing),
            Self::InputSatisfied(mp) => (mp, Disposition::InputSatisfied),
            Self::Finished(mp) => (mp, Disposition::Finished),
        }
    }
}

pub enum StreamingSinkFinalizeOutput<Op: StreamingSink> {
    HasMoreOutput {
        states: Vec<Op::State>,
        output: Option<MicroPartition>,
    },
    Finished(Option<MicroPartition>),
}

pub(crate) type StreamingSinkExecuteResult<Op> =
    OperatorOutput<DaftResult<(<Op as StreamingSink>::State, StreamingSinkOutput)>>;
pub(crate) type StreamingSinkFinalizeResult<Op> =
    OperatorOutput<DaftResult<StreamingSinkFinalizeOutput<Op>>>;

pub(crate) trait StreamingSink: Send + Sync {
    type State: Send + Sync + Unpin;
    type Stats: RuntimeStats = DefaultRuntimeStats;
    type BatchingStrategy: BatchingStrategy + 'static;

    fn execute(
        &self,
        input: MicroPartition,
        state: Self::State,
        runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self>;

    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self>
    where
        Self: Sized;

    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State>;
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }
    fn batching_strategy(&self) -> Self::BatchingStrategy;

    /// Whether this operator ever returns
    /// [`StreamingSinkOutput::InputSatisfied`].
    ///
    /// Opting in gives the operator's subtree its own cancellation scope, so
    /// satisfying one input cannot stop a producer feeding a *sibling* branch —
    /// the other side of a broadcast or cross join fused into the same task plan
    /// by `SwordfishTaskBuilder::combine_with`. Operators that never return
    /// `InputSatisfied` leave this at `false` and simply forward the scope they
    /// were handed.
    fn cancels_inputs(&self) -> bool {
        false
    }
}

enum TaskResult<Op: StreamingSink> {
    Execute(InputId, Op::State, StreamingSinkOutput, Duration),
    Completed,
}

struct PerInputState<Op: StreamingSink> {
    states: Vec<Op::State>,
    runtime_stats: Arc<Op::Stats>,
    max_concurrency: usize,
}

impl<Op: StreamingSink + 'static> PerInputState<Op> {
    fn new(
        op: &Arc<Op>,
        input_id: InputId,
        max_concurrency: usize,
        runtime_stats: Arc<Op::Stats>,
    ) -> DaftResult<Self> {
        let states = (0..max_concurrency)
            .map(|_| op.make_state(input_id))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            states,
            runtime_stats,
            max_concurrency,
        })
    }

    fn all_states_idle(&self) -> bool {
        self.states.len() == self.max_concurrency
    }
}

// ========== ExecutionContext ==========

struct ExecutionContext<Op: StreamingSink> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    finalize_spawner: ExecutionTaskSpawner,
    task_set: OrderingAwareJoinSet<DaftResult<TaskResult<Op>>>,
    output_sender: Sender<PipelineMessage>,
    batch_manager: BatchManager<Op::BatchingStrategy>,
    inputs: HashMap<InputId, PerInputState<Op>>,
    stats_manager: RuntimeStatsManagerHandle,
    meter: Meter,
    node_info: Arc<NodeInfo>,
    node_id: usize,
    node_initialized: bool,
    /// The scope covering this node's *child* subtree — the producers of the
    /// data this node consumes. Owned (and therefore writable) only when
    /// `op.cancels_inputs()`; otherwise it is the scope inherited from a
    /// cancelling sink further downstream, which this node only reads.
    input_cancel: InputCancelRegistry,
    owns_input_cancel: bool,
}

impl<Op: StreamingSink + 'static> ExecutionContext<Op> {
    fn get_or_create_input(&mut self, input_id: InputId) -> DaftResult<&mut PerInputState<Op>> {
        let max_concurrency = self.op.max_concurrency();
        match self.inputs.entry(input_id) {
            Entry::Occupied(e) => Ok(e.into_mut()),
            Entry::Vacant(e) => {
                let runtime_stats = Arc::new(Op::Stats::new(&self.meter, &self.node_info));
                self.stats_manager.register_runtime_stats(
                    self.node_id,
                    input_id,
                    runtime_stats.clone(),
                );
                Ok(e.insert(PerInputState::new(
                    &self.op,
                    input_id,
                    max_concurrency,
                    runtime_stats,
                )?))
            }
        }
    }

    fn dispatch_ready_batches(&mut self, input_id: InputId) -> DaftResult<()> {
        if self.input_cancel.is_cancelled(input_id) {
            // Satisfied — by this operator, or by a sink downstream of it.
            // Discard rather than `drain`: the entry has to stay so the input's
            // `Flush` is still handled as an ordinary one.
            self.batch_manager.discard_buffered(input_id)?;
            return Ok(());
        }
        let per_input = self
            .inputs
            .get_mut(&input_id)
            .expect("Input should be present");
        while let Some(state) = per_input.states.pop() {
            if let Some(batch) = self.batch_manager.next_batch(input_id)? {
                let op = self.op.clone();
                let spawner = self.task_spawner.clone();
                let runtime_stats = per_input.runtime_stats.clone();
                self.task_set.spawn(async move {
                    let now = Instant::now();
                    let (state, output) =
                        op.execute(batch, state, runtime_stats, &spawner).await??;
                    Ok(TaskResult::Execute(input_id, state, output, now.elapsed()))
                });
            } else {
                per_input.states.push(state);
                break;
            }
        }
        Ok(())
    }

    fn try_complete_input(&mut self, input_id: InputId) -> DaftResult<()> {
        let workers_idle = self
            .inputs
            .get(&input_id)
            .is_some_and(|p| p.all_states_idle());
        if workers_idle && self.batch_manager.can_flush(input_id) {
            let remaining = self.batch_manager.drain(input_id)?;
            let per_input = self.inputs.remove(&input_id).unwrap();
            if self.owns_input_cancel {
                // The subtree has finished this input, so nothing can act on
                // the flag any more. Dropping it keeps the registry from growing
                // by an entry per task on a long-lived reused pipeline. Only the
                // owner may do this: clearing a flag while a sibling branch is
                // still producing would let that branch resume.
                self.input_cancel.forget(input_id);
            }
            self.spawn_complete_input(per_input, remaining, input_id);
        }
        Ok(())
    }

    fn spawn_complete_input(
        &mut self,
        per_input: PerInputState<Op>,
        remaining: Option<MicroPartition>,
        input_id: InputId,
    ) {
        let op = self.op.clone();
        let task_spawner = self.task_spawner.clone();
        let finalize_spawner = self.finalize_spawner.clone();
        let output_tx = self.output_sender.clone();
        self.task_set.spawn(async move {
            let PerInputState {
                mut states,
                runtime_stats,
                ..
            } = per_input;

            if let Some(partition) = remaining {
                let state = states.pop().expect("must have at least one idle state");
                let now = Instant::now();
                let (new_state, result) = op
                    .execute(partition, state, runtime_stats.clone(), &task_spawner)
                    .await??;
                let elapsed = now.elapsed();
                runtime_stats.add_duration_us(elapsed.as_micros() as u64);
                runtime_stats.increment_num_tasks();

                // `InputSatisfied` needs no special handling here: this is
                // already the input's last batch (`try_complete_input` only
                // fires once the child has flushed it), so there is nothing
                // left upstream to cancel and the input proceeds to finalize
                // just as `NeedMoreInput` would.
                let (mp, finished) = match result.split() {
                    (mp, Disposition::Finished) => (mp, true),
                    (mp, _) => (mp, false),
                };
                if let Some(mp) = mp {
                    runtime_stats.add_rows_out(mp.len() as u64);
                    runtime_stats.add_bytes_out(mp.size_bytes() as u64);
                    let _ = output_tx
                        .send(PipelineMessage::Morsel {
                            input_id,
                            partition: mp,
                        })
                        .await;
                }
                if finished {
                    let _ = output_tx.send(PipelineMessage::Flush(input_id)).await;
                    return Ok(TaskResult::Completed);
                }
                states.push(new_state);
            }

            let mut finalize_states = states;
            loop {
                match op.finalize(finalize_states, &finalize_spawner).await?? {
                    StreamingSinkFinalizeOutput::HasMoreOutput {
                        states: new_states,
                        output,
                    } => {
                        if let Some(mp) = output {
                            runtime_stats.add_rows_out(mp.len() as u64);
                            runtime_stats.add_bytes_out(mp.size_bytes() as u64);
                            let _ = output_tx
                                .send(PipelineMessage::Morsel {
                                    input_id,
                                    partition: mp,
                                })
                                .await;
                        }
                        finalize_states = new_states;
                    }
                    StreamingSinkFinalizeOutput::Finished(output) => {
                        if let Some(mp) = output {
                            runtime_stats.add_rows_out(mp.len() as u64);
                            runtime_stats.add_bytes_out(mp.size_bytes() as u64);
                            let _ = output_tx
                                .send(PipelineMessage::Morsel {
                                    input_id,
                                    partition: mp,
                                })
                                .await;
                        }
                        break;
                    }
                }
            }

            let _ = output_tx.send(PipelineMessage::Flush(input_id)).await;
            Ok(TaskResult::Completed)
        });
    }

    async fn handle_worker_result(
        &mut self,
        input_id: InputId,
        state: Op::State,
        output: StreamingSinkOutput,
        elapsed: Duration,
    ) -> DaftResult<ControlFlow<(), ()>> {
        let per_input = self.inputs.get_mut(&input_id).unwrap();
        per_input
            .runtime_stats
            .add_duration_us(elapsed.as_micros() as u64);
        per_input.runtime_stats.increment_num_tasks();

        let (mp, disposition) = output.split();
        self.batch_manager.record_completion(
            per_input.runtime_stats.as_ref(),
            mp.as_ref().map(|p| p.len()).unwrap_or(0),
            elapsed,
        );
        if let Some(mp) = mp {
            per_input.runtime_stats.add_rows_out(mp.len() as u64);
            per_input
                .runtime_stats
                .add_bytes_out(mp.size_bytes() as u64);
            if self
                .output_sender
                .send(PipelineMessage::Morsel {
                    input_id,
                    partition: mp,
                })
                .await
                .is_err()
            {
                return Ok(ControlFlow::Break(()));
            }
        }

        if disposition == Disposition::Finished {
            let _ = self
                .output_sender
                .send(PipelineMessage::Flush(input_id))
                .await;
            return Ok(ControlFlow::Break(()));
        }

        per_input.states.push(state);

        if disposition == Disposition::InputSatisfied {
            debug_assert!(
                self.owns_input_cancel,
                "operator returned InputSatisfied without opting into cancels_inputs"
            );
            // Stop the producers feeding this input. `dispatch_ready_batches`
            // then throws away whatever they already handed us instead of
            // claiming against it. The input still completes normally: its
            // `Flush` arrives once the short-circuited subtree drains, and
            // `try_complete_input` takes it from there.
            self.input_cancel.cancel(input_id);
        }
        self.dispatch_ready_batches(input_id)?;
        self.try_complete_input(input_id)?;
        Ok(ControlFlow::Continue(()))
    }

    async fn process_input(&mut self, receiver: &mut Receiver<PipelineMessage>) -> DaftResult<()> {
        let max_concurrency = self.op.max_concurrency();
        let mut child_closed = false;

        while let Some(event) = next_event(
            &mut self.task_set,
            max_concurrency,
            receiver,
            &mut child_closed,
        )
        .await?
        {
            let cf = match event {
                PipelineEvent::TaskCompleted(TaskResult::Completed) => ControlFlow::Continue(()),
                PipelineEvent::TaskCompleted(TaskResult::Execute(
                    input_id,
                    state,
                    output,
                    elapsed,
                )) => {
                    self.handle_worker_result(input_id, state, output, elapsed)
                        .await?
                }
                PipelineEvent::Morsel {
                    input_id,
                    partition,
                } => {
                    if !self.node_initialized {
                        self.stats_manager.activate_node(self.node_id);
                        self.node_initialized = true;
                    }
                    let per_input = self.get_or_create_input(input_id)?;
                    per_input.runtime_stats.add_rows_in(partition.len() as u64);
                    per_input
                        .runtime_stats
                        .add_bytes_in(partition.size_bytes() as u64);
                    self.batch_manager.push(input_id, partition);
                    self.dispatch_ready_batches(input_id)?;
                    ControlFlow::Continue(())
                }
                PipelineEvent::Flush(input_id) => {
                    if !self.node_initialized {
                        self.stats_manager.activate_node(self.node_id);
                        self.node_initialized = true;
                    }
                    self.batch_manager.set_pending_flush(input_id);
                    if self.inputs.contains_key(&input_id) {
                        self.dispatch_ready_batches(input_id)?;
                    }
                    self.try_complete_input(input_id)?;
                    ControlFlow::Continue(())
                }
                PipelineEvent::FlightPartitionRef => {
                    unreachable!(
                        "StreamingSinkNode should not receive flight partition refs from child"
                    )
                }
                PipelineEvent::InputClosed => {
                    self.batch_manager.set_all_pending_flush();
                    let input_ids: Vec<_> = self.inputs.keys().copied().collect();
                    for input_id in input_ids {
                        self.dispatch_ready_batches(input_id)?;
                        self.try_complete_input(input_id)?;
                    }
                    ControlFlow::Continue(())
                }
            };
            if cf.is_break() {
                return Ok(());
            }
        }
        Ok(())
    }
}

// ========== StreamingSinkNode ==========

pub struct StreamingSinkNode<Op: StreamingSink> {
    op: Arc<Op>,
    child: Box<dyn PipelineNode>,
    meter: Meter,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
    morsel_size_requirement: MorselSizeRequirement,
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
        let morsel_size_requirement = op.morsel_size_requirement().unwrap_or_default();
        Self {
            op,
            child,
            meter: ctx.meter.clone(),
            plan_stats,
            node_info: Arc::new(node_info),
            morsel_size_requirement,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
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
            _ => {
                let multiline_display = self.op.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
                writeln!(display, "Batch Size = {}", self.morsel_size_requirement).unwrap();
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

        let mut json = serde_json::json!({
            "id": self.node_id(),
            "category": "StreamingSink",
            "type": self.op.op_type().to_string(),
            "name": self.name(),
            "children": children,
        });

        if let StatsState::Materialized(stats) = &self.plan_stats {
            json["approx_stats"] = serde_json::json!(stats);
        }

        json
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
        default_morsel_size: MorselSizeRequirement,
    ) {
        let operator_morsel_size_requirement = self.op.morsel_size_requirement();
        let combined_morsel_size_requirement = MorselSizeRequirement::combine_requirements(
            operator_morsel_size_requirement,
            downstream_requirement,
        );
        self.morsel_size_requirement = combined_morsel_size_requirement;
        self.child.propagate_morsel_size_requirement(
            combined_morsel_size_requirement,
            default_morsel_size,
        );
    }

    fn start(
        self: Box<Self>,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
        input_cancel: &InputCancelRegistry,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let Self {
            op,
            child,
            meter,
            node_info,
            ..
        } = *self;
        let node_id = node_info.id;
        let name: Arc<str> = node_info.name.clone();
        // A cancelling operator gets a scope of its own, covering exactly the
        // producers it consumes from; everything else forwards what it was
        // handed, so a cancel from further downstream still reaches down here.
        let owns_input_cancel = op.cancels_inputs();
        let input_cancel = if owns_input_cancel {
            InputCancelRegistry::new()
        } else {
            input_cancel.clone()
        };
        let child_rx = child.start(maintain_order, runtime_handle, &input_cancel)?;
        let (output_tx, output_rx) = create_channel(1);
        let memory_manager = runtime_handle.memory_manager();
        let stats_manager = runtime_handle.stats_manager();
        let batch_manager = BatchManager::new(op.batching_strategy());

        let compute_runtime = get_compute_runtime();
        let task_spawner = ExecutionTaskSpawner::new(
            compute_runtime.clone(),
            memory_manager.clone(),
            info_span!("StreamingSink::Execute"),
        );
        let finalize_spawner = ExecutionTaskSpawner::new(
            compute_runtime,
            memory_manager,
            info_span!("StreamingSink::Finalize"),
        );

        runtime_handle.spawn(
            async move {
                let mut ctx = ExecutionContext {
                    op,
                    task_spawner,
                    finalize_spawner,
                    task_set: OrderingAwareJoinSet::new(maintain_order),
                    output_sender: output_tx,
                    batch_manager,
                    inputs: HashMap::new(),
                    stats_manager: stats_manager.clone(),
                    meter,
                    node_info,
                    node_id,
                    node_initialized: false,
                    input_cancel,
                    owns_input_cancel,
                };
                let mut child_rx = child_rx;
                ctx.process_input(&mut child_rx).await?;
                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &name,
        );

        Ok(output_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
    fn node_id(&self) -> usize {
        self.node_info.id
    }
    fn node_info(&self) -> Arc<NodeInfo> {
        self.node_info.clone()
    }
}
