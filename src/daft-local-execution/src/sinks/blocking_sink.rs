use std::{collections::HashMap, num::NonZeroUsize, sync::Arc, time::Instant};

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::{DaftError, DaftResult};
use common_metrics::{
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::StatSnapshotImpl,
};
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads, get_compute_runtime};
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender, create_channel},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    pipeline_execution::{PipelineEvent, next_event},
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats},
};

pub(crate) type BlockingSinkSinkResult<Op> =
    OperatorOutput<DaftResult<<Op as BlockingSink>::State>>;
pub(crate) type BlockingSinkFinalizeResult = OperatorOutput<DaftResult<Vec<Arc<MicroPartition>>>>;
pub(crate) trait BlockingSink: Send + Sync {
    type State: Send + Sync + Unpin;

    fn sink(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self>
    where
        Self: Sized;
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult
    where
        Self: Sized;
    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> DaftResult<Self::State>;
    fn make_runtime_stats(&self, name: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(DefaultRuntimeStats::new(name))
    }
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
}

/// Per-node task result for blocking sinks.
enum BlockingSinkTaskResult<S> {
    Sink {
        state: S,
        elapsed: std::time::Duration,
    },
    Finalize {
        input_id: InputId,
        output: Vec<Arc<MicroPartition>>,
    },
}

/// Dedicated processor for a single input_id. Each processor owns its own
/// task_set and states — mirroring independent pipeline behavior at the
/// blocking sink level while sharing the rest of the pipeline.
struct SingleInputBlockingSinkProcessor<Op: BlockingSink> {
    input_id: InputId,
    task_set: OrderingAwareJoinSet<DaftResult<BlockingSinkTaskResult<Op::State>>>,
    max_concurrency: usize,
    states: Vec<Op::State>,
    total_states: usize,
    buffer: RowBasedBuffer,
    is_completed: bool,
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    finalize_spawner: ExecutionTaskSpawner,
    runtime_stats: Arc<dyn RuntimeStats>,
    output_sender: Sender<PipelineMessage>,
    op_name: Arc<str>,
}

impl<Op: BlockingSink + 'static> SingleInputBlockingSinkProcessor<Op> {
    fn spawn_sink_task(&mut self, partition: Arc<MicroPartition>, state: Op::State) {
        let op = self.op.clone();
        let task_spawner = self.task_spawner.clone();
        self.task_set.spawn(async move {
            let now = Instant::now();
            let new_state = op.sink(partition, state, &task_spawner).await??;
            let elapsed = now.elapsed();
            Ok(BlockingSinkTaskResult::Sink {
                state: new_state,
                elapsed,
            })
        });
    }

    fn try_progress(&mut self) -> DaftResult<()> {
        // Try finalize first: all states returned, buffer empty, flush received
        if self.is_completed
            && self.states.len() == self.total_states
            && self.buffer.is_empty()
        {
            let states = std::mem::take(&mut self.states);
            let op = self.op.clone();
            let finalize_spawner = self.finalize_spawner.clone();
            let input_id = self.input_id;
            self.task_set.spawn(async move {
                let output = op.finalize(states, &finalize_spawner).await??;
                Ok(BlockingSinkTaskResult::Finalize { input_id, output })
            });
            return Ok(());
        }

        // Fill remaining task slots with sink tasks
        while self.task_set.len() < self.max_concurrency
            && !self.states.is_empty()
            && !self.buffer.is_empty()
        {
            if let Some(partition) = self.buffer.pop_all()? {
                let state = self.states.pop().unwrap();
                self.spawn_sink_task(partition, state);
            }
        }
        Ok(())
    }

    /// Run the event loop for a single input_id. Reads morsels/flushes from
    /// `receiver`, processes them, and sends finalized output to `output_sender`.
    async fn run(mut self, mut receiver: Receiver<PipelineMessage>) -> DaftResult<()> {
        let mut input_closed = false;
        let start_time = Instant::now();

        loop {
            // Done when: input closed, no in-flight tasks, and either finalized or nothing to do
            if input_closed && self.task_set.is_empty() {
                if !self.is_completed {
                    // Input channel closed without flush — mark completed
                    self.is_completed = true;
                    self.try_progress()?;
                    if self.task_set.is_empty() {
                        break;
                    }
                } else if self.states.len() == self.total_states && self.buffer.is_empty() {
                    break;
                }
            }

            let event = next_event(
                &mut self.task_set,
                self.max_concurrency,
                &mut receiver,
                &mut input_closed,
            )
            .await?;

            match event {
                PipelineEvent::TaskCompleted(BlockingSinkTaskResult::Sink {
                    state, elapsed,
                }) => {
                    println!(
                        "[Daft] [{:.3}] {} sink_task completed input_id={} compute={:.3}s tasks={}",
                        crate::epoch_secs(),
                        self.op_name,
                        self.input_id,
                        elapsed.as_secs_f64(),
                        self.task_set.len(),
                    );
                    self.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);
                    self.states.push(state);
                    self.try_progress()?;
                }
                PipelineEvent::TaskCompleted(BlockingSinkTaskResult::Finalize {
                    input_id,
                    output,
                }) => {
                    let output_rows: usize = output.iter().map(|p| p.len()).sum();
                    println!(
                        "[Daft] [{:.3}] {} finalize_task completed input_id={} rows={} tasks={}",
                        crate::epoch_secs(),
                        self.op_name,
                        input_id,
                        output_rows,
                        self.task_set.len(),
                    );
                    for partition in &output {
                        self.runtime_stats.add_rows_out(partition.len() as u64);
                        if self
                            .output_sender
                            .send(PipelineMessage::Morsel {
                                input_id,
                                partition: partition.clone(),
                            })
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    println!(
                        "[Daft] [{:.3}] {} input_id={} finished in {:.3}s",
                        crate::epoch_secs(),
                        self.op_name,
                        input_id,
                        start_time.elapsed().as_secs_f64(),
                    );
                    if self
                        .output_sender
                        .send(PipelineMessage::Flush(input_id))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    return Ok(()); // Done with this input_id
                }
                PipelineEvent::Morsel { partition, .. } => {
                    println!(
                        "[Daft] [{:.3}] {} morsel input_id={} rows={} tasks={}",
                        crate::epoch_secs(),
                        self.op_name,
                        self.input_id,
                        partition.len(),
                        self.task_set.len(),
                    );
                    self.runtime_stats.add_rows_in(partition.len() as u64);
                    self.buffer.push(partition);
                    self.try_progress()?;
                }
                PipelineEvent::Flush(_) => {
                    println!(
                        "[Daft] [{:.3}] {} flush input_id={} tasks={}",
                        crate::epoch_secs(),
                        self.op_name,
                        self.input_id,
                        self.task_set.len(),
                    );
                    self.is_completed = true;
                    self.try_progress()?;
                }
                PipelineEvent::InputClosed => {
                    println!(
                        "[Daft] [{:.3}] {} input_closed input_id={} tasks={}",
                        crate::epoch_secs(),
                        self.op_name,
                        self.input_id,
                        self.task_set.len(),
                    );
                    self.is_completed = true;
                    self.try_progress()?;
                }
            }
        }
        Ok(())
    }
}

pub struct BlockingSinkNode<Op: BlockingSink> {
    op: Arc<Op>,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
}

impl<Op: BlockingSink + 'static> BlockingSinkNode<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        child: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = op.name().into();
        let node_info = ctx.next_node_info(name, op.op_type(), NodeCategory::BlockingSink, context);
        let runtime_stats = op.make_runtime_stats(node_info.id);

        Self {
            op,
            child,
            runtime_stats,
            plan_stats,
            node_info: Arc::new(node_info),
        }
    }
    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

impl<Op: BlockingSink + 'static> TreeDisplay for BlockingSinkNode<Op> {
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
            "category": "BlockingSink",
            "type": self.op.op_type().to_string(),
            "name": self.name(),
            "children": children,
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

impl<Op: BlockingSink + 'static> PipelineNode for BlockingSinkNode<Op> {
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
        _downstream_requirement: MorselSizeRequirement,
        default_morsel_size: MorselSizeRequirement,
    ) {
        self.child
            .propagate_morsel_size_requirement(default_morsel_size, default_morsel_size);
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let mut child_results_receiver = self.child.start(false, runtime_handle)?;

        let (destination_sender, destination_receiver) = create_channel(4);

        let task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("BlockingSink::Sink"),
        );
        let finalize_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("BlockingSink::Finalize"),
        );

        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let node_id = self.node_id();
        let op_name = self.name();
        let stats_manager = runtime_handle.stats_manager();
        runtime_handle.spawn(
            async move {
                let mut per_input_senders: HashMap<InputId, Sender<PipelineMessage>> =
                    HashMap::new();
                let mut processor_set =
                    tokio::task::JoinSet::<DaftResult<()>>::new();
                let mut node_initialized = false;
                let mut input_closed = false;

                loop {
                    // Done when input is closed and all processors have finished
                    if input_closed && processor_set.is_empty() {
                        break;
                    }

                    tokio::select! {
                        // Branch 1: receive from child pipeline
                        msg = child_results_receiver.recv(), if !input_closed => {
                            let Some(msg) = msg else {
                                // Input channel closed: drop all per-input senders
                                // so processor channels close
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

                            if !per_input_senders.contains_key(&input_id) {
                                // First message for this input_id: spawn a processor
                                println!(
                                    "[Daft] [{:.3}] {} spawning processor for input_id={}",
                                    crate::epoch_secs(),
                                    op_name,
                                    input_id,
                                );
                                let (tx, rx) = create_channel(4);
                                per_input_senders.insert(input_id, tx);

                                let states = (0..op.max_concurrency())
                                    .map(|_| op.make_state())
                                    .collect::<DaftResult<Vec<_>>>()?;
                                let total_states = states.len();

                                let processor = SingleInputBlockingSinkProcessor {
                                    input_id,
                                    task_set: OrderingAwareJoinSet::new(maintain_order),
                                    max_concurrency: get_compute_pool_num_threads(),
                                    states,
                                    total_states,
                                    buffer: RowBasedBuffer::new(
                                        0,
                                        NonZeroUsize::new(usize::MAX).unwrap(),
                                    ),
                                    is_completed: false,
                                    op: op.clone(),
                                    task_spawner: task_spawner.clone(),
                                    finalize_spawner: finalize_spawner.clone(),
                                    runtime_stats: runtime_stats.clone(),
                                    output_sender: destination_sender.clone(),
                                    op_name: op_name.clone(),
                                };

                                processor_set.spawn(async move {
                                    processor.run(rx).await
                                });
                            }

                            // Route message to per-input processor
                            let is_flush = matches!(&msg, PipelineMessage::Flush(_));
                            if per_input_senders[&input_id].send(msg).await.is_err() {
                                // Processor died — error will surface from join below
                            }
                            // After flush, drop the sender so the processor's channel closes
                            if is_flush {
                                per_input_senders.remove(&input_id);
                            }
                        }
                        // Branch 2: a processor completed (or errored)
                        Some(result) = processor_set.join_next(), if !processor_set.is_empty() => {
                            result.map_err(DaftError::JoinError)??;
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
