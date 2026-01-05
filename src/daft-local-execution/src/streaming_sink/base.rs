use std::{collections::HashMap, sync::Arc, time::Instant};

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeInfo, NodeType};
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_core::prelude::SchemaRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    channel::{Receiver, Sender, create_channel},
    dispatcher::{DispatchSpawner, DynamicUnorderedDispatcher},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    plan_input::{InputId, PipelineMessage},
    resource_manager::MemoryManager,
    runtime_stats::{
        CountingSender, DefaultRuntimeStats, InitializingCountingReceiver, RuntimeStats,
    },
};

/// Unified message type for streaming sink workers that combines
/// morsel output and flush state information
enum StreamingSinkWorkerMessage<State> {
    Morsel {
        input_id: InputId,
        partition: Arc<MicroPartition>,
    },
    FlushState(InputId, Option<State>),
}

pub enum StreamingSinkOutput {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput(Option<Arc<MicroPartition>>),
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
    /// received from the child with the given index,
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
    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(DefaultRuntimeStats::new(id))
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
    fn dispatch_spawner(
        &self,
        batch_manager: Arc<BatchManager<Self::BatchingStrategy>>,
    ) -> Arc<dyn DispatchSpawner> {
        Arc::new(DynamicUnorderedDispatcher::new(batch_manager))
    }
}

pub struct StreamingSinkNode<Op: StreamingSink> {
    op: Arc<Op>,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
    morsel_size_requirement: MorselSizeRequirement,
}

impl<Op: StreamingSink + 'static> StreamingSinkNode<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        child: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
        output_schema: SchemaRef,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = op.name().into();
        let node_info = ctx.next_node_info(
            name,
            op.op_type(),
            NodeCategory::StreamingSink,
            output_schema,
            context,
        );
        let runtime_stats = op.make_runtime_stats(node_info.id);

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

    async fn finalize_and_send_output(
        op: Arc<Op>,
        states: Vec<Op::State>,
        input_id: InputId,
        memory_manager: Arc<MemoryManager>,
        runtime_stats: Arc<dyn RuntimeStats>,
        counting_sender: &CountingSender,
    ) -> DaftResult<bool> {
        println!("[finalize_and_send_output] Called for input_id: {input_id:?}, states.len()={}", states.len());
        let compute_runtime = get_compute_runtime();
        let spawner = ExecutionTaskSpawner::new(
            compute_runtime,
            memory_manager,
            runtime_stats,
            info_span!("StreamingSink::Finalize"),
        );

        let mut current_states = states;
        loop {
            println!("[finalize_and_send_output] About to call op.finalize for input_id: {input_id:?}, current_states.len()={}", current_states.len());
            let finalized_result = op.finalize(current_states, &spawner).await??;
            match finalized_result {
                StreamingSinkFinalizeOutput::HasMoreOutput { states, output } => {
                    println!("[finalize_and_send_output] Got HasMoreOutput for input_id: {input_id:?}, output.is_some()={}", output.is_some());
                    if let Some(mp) = output
                        && counting_sender
                            .send(PipelineMessage::Morsel {
                                input_id,
                                partition: mp,
                            })
                            .await
                            .is_err()
                    {
                        println!("[finalize_and_send_output] Counting sender failed to send morsel -- returning false");
                        return Ok(false);
                    }
                    current_states = states;
                }
                StreamingSinkFinalizeOutput::Finished(output) => {
                    println!("[finalize_and_send_output] Got Finished for input_id: {input_id:?}, output.is_some()={}", output.is_some());
                    if let Some(mp) = output
                        && counting_sender
                            .send(PipelineMessage::Morsel {
                                input_id,
                                partition: mp,
                            })
                            .await
                            .is_err()
                    {
                        println!("[finalize_and_send_output] Counting sender failed to send finished morsel -- returning false");
                        return Ok(false);
                    }

                    // Send flush signal after finalizing this input_id
                    if counting_sender
                        .send(PipelineMessage::Flush(input_id))
                        .await
                        .is_err()
                    {
                        println!("[finalize_and_send_output] Counting sender failed to send flush -- returning false");
                        return Ok(false);
                    }
                    println!("[finalize_and_send_output] Done for input_id: {input_id:?}");
                    break;
                }
            }
        }
        Ok(true)
    }

    #[instrument(level = "info", skip_all, name = "StreamingSink::run_worker")]
    async fn run_worker(
        op: Arc<Op>,
        mut input_receiver: Receiver<PipelineMessage>,
        output_sender: Sender<StreamingSinkWorkerMessage<Op::State>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        memory_manager: Arc<MemoryManager>,
        batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    ) -> DaftResult<()> {
        println!(
            "[run_worker] Worker started for op '{}'",
            op.name()
        );
        let span = info_span!("StreamingSink::Execute");
        let compute_runtime = get_compute_runtime();
        let spawner =
            ExecutionTaskSpawner::new(compute_runtime, memory_manager, runtime_stats.clone(), span);
        let mut states: HashMap<InputId, Op::State> = HashMap::new();
        'outer: while let Some(msg) = input_receiver.recv().await {
            match msg {
                PipelineMessage::Morsel {
                    input_id,
                    partition,
                } => {
                    println!(
                        "[run_worker] Received morsel - input_id: {:?}, partition_len: {}",
                        input_id,
                        partition.len()
                    );
                    // Get or create state for this input ID

                    loop {
                        let state = match states.remove(&input_id) {
                            Some(state) => state,
                            None => {
                                println!(
                                    "[run_worker] No state for input_id {:?}, calling op.make_state()",
                                    input_id
                                );
                                op.make_state()?
                            },
                        };
                        println!(
                            "[run_worker] Calling op.execute for input_id {:?}",
                            input_id
                        );
                        let now = Instant::now();
                        let (new_state, output) =
                            op.execute(partition.clone(), state, &spawner).await??;
                        let elapsed = now.elapsed();
                        println!(
                            "[run_worker] op.execute returned for input_id {:?} in {:?}",
                            input_id, elapsed
                        );
                        states.insert(input_id, new_state);

                        match output {
                            StreamingSinkOutput::NeedMoreInput(mp) => {
                                println!(
                                    "[run_worker] Got NeedMoreInput for input_id {:?}, mp.is_some()={}",
                                    input_id,
                                    mp.is_some()
                                );
                                batch_manager.record_execution_stats(
                                    runtime_stats.clone(),
                                    mp.as_ref().map(|mp| mp.len()).unwrap_or(0),
                                    elapsed,
                                );
                                if let Some(mp) = mp
                                    && output_sender
                                        .send(StreamingSinkWorkerMessage::Morsel {
                                            input_id,
                                            partition: mp,
                                        })
                                        .await
                                        .is_err()
                                {
                                    println!("[run_worker] output_sender failed NeedMoreInput");
                                    break 'outer;
                                }
                                break;
                            }
                            StreamingSinkOutput::HasMoreOutput(mp) => {
                                println!(
                                    "[run_worker] Got HasMoreOutput for input_id {:?}, mp.is_some()={}",
                                    input_id,
                                    mp.is_some()
                                );
                                batch_manager.record_execution_stats(
                                    runtime_stats.clone(),
                                    mp.as_ref().map(|mp| mp.len()).unwrap_or(0),
                                    elapsed,
                                );
                                if let Some(mp) = mp
                                    && output_sender
                                        .send(StreamingSinkWorkerMessage::Morsel {
                                            input_id,
                                            partition: mp,
                                        })
                                        .await
                                        .is_err()
                                {
                                    println!("[run_worker] output_sender failed HasMoreOutput");
                                    break 'outer;
                                }
                                // Continue loop with updated state
                            }
                            StreamingSinkOutput::Finished(mp) => {
                                println!(
                                    "[run_worker] Got Finished for input_id {:?}, mp.is_some()={}",
                                    input_id,
                                    mp.is_some()
                                );
                                batch_manager.record_execution_stats(
                                    runtime_stats.clone(),
                                    mp.as_ref().map(|mp| mp.len()).unwrap_or(0),
                                    elapsed,
                                );
                                if let Some(mp) = mp
                                    && output_sender
                                        .send(StreamingSinkWorkerMessage::Morsel {
                                            input_id,
                                            partition: mp,
                                        })
                                        .await
                                        .is_err()
                                {
                                    println!("[run_worker] output_sender failed Finished");
                                    break 'outer;
                                }
                                // Exit outer loop, flush everything, and return
                                break 'outer;
                            }
                        }
                    }
                }
                PipelineMessage::Flush(input_id) => {
                    println!(
                        "[run_worker] Received Flush for input_id {:?}",
                        input_id
                    );
                    // Send state back to coordinator when flush is called
                    // Always send, even if state doesn't exist for this input_id
                    let state = states.remove(&input_id);
                    if output_sender
                        .send(StreamingSinkWorkerMessage::FlushState(input_id, state))
                        .await
                        .is_err()
                    {
                        println!(
                            "[run_worker] output_sender failed sending FlushState for input_id {:?}",
                            input_id
                        );
                        break;
                    }
                }
            }
        }
        // Input receiver is exhausted or Finished was returned, send all remaining states
        for (input_id, state) in states {
            println!(
                "[run_worker] input receiver exhausted or finished, sending remaining state FlushState for input_id {:?}",
                input_id
            );
            if output_sender
                .send(StreamingSinkWorkerMessage::FlushState(input_id, Some(state)))
                .await
                .is_err()
            {
                println!("[run_worker] output_sender failed sending remaining FlushState for input_id {:?}", input_id);
                break;
            }
        }

        println!("[run_worker] Worker finishing!");
        Ok(())
    }

    fn spawn_workers(
        op: Arc<Op>,
        input_receivers: Vec<Receiver<PipelineMessage>>,
        runtime_handle: &mut ExecutionRuntimeContext,
        runtime_stats: Arc<dyn RuntimeStats>,
        memory_manager: Arc<MemoryManager>,
        batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    ) -> Receiver<StreamingSinkWorkerMessage<Op::State>> {
        let (output_sender, output_receiver) = create_channel(input_receivers.len());

        for input_receiver in input_receivers {
            println!(
                "[spawn_workers] Spawning worker for op '{}'",
                op.name()
            );
            runtime_handle.spawn(
                Self::run_worker(
                    op.clone(),
                    input_receiver,
                    output_sender.clone(),
                    runtime_stats.clone(),
                    memory_manager.clone(),
                    batch_manager.clone(),
                ),
                &op.name(),
            );
        }
        output_receiver
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
                    for (name, value) in rt_result {
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
        &self,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        println!(
            "[start] Starting StreamingSinkNode '{}' (node_id={})",
            self.name(),
            self.node_id()
        );
        let child_result_receiver = self.child.start(runtime_handle)?;
        let child_result_receiver = InitializingCountingReceiver::new(
            child_result_receiver,
            self.node_id(),
            self.runtime_stats.clone(),
            runtime_handle.stats_manager(),
        );

        let (destination_sender, destination_receiver) = create_channel(1);
        let counting_sender = CountingSender::new(destination_sender, self.runtime_stats.clone());

        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let num_workers = op.max_concurrency();
        println!(
            "[start] Operator '{}' will launch {} workers.",
            op.name(),
            num_workers
        );
        let strategy = op.batching_strategy();
        let batch_manager = Arc::new(BatchManager::new(strategy));
        let dispatch_spawner = op.dispatch_spawner(batch_manager.clone());
        let spawned_dispatch_result = dispatch_spawner.spawn_dispatch(
            child_result_receiver,
            num_workers,
            &mut runtime_handle.handle(),
        );
        runtime_handle.spawn(
            async move { spawned_dispatch_result.spawned_dispatch_task.await? },
            &self.name(),
        );

        // Spawn workers on runtime_handle
        let num_workers_spawned = spawned_dispatch_result.worker_receivers.len();
        println!(
            "[start] Actually spawning {} workers for operator '{}'.",
            num_workers_spawned,
            op.name()
        );
        let mut output_receiver = Self::spawn_workers(
            op.clone(),
            spawned_dispatch_result.worker_receivers,
            runtime_handle,
            self.runtime_stats.clone(),
            runtime_handle.memory_manager(),
            batch_manager.clone(),
        );

        let memory_manager = runtime_handle.memory_manager();
        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();
        runtime_handle.spawn(
            async move {
                // Track states and flush counts per input_id
                let mut all_states: HashMap<InputId, Vec<Op::State>> = HashMap::new();
                let mut flush_counts: HashMap<InputId, usize> = HashMap::new();

                while let Some(msg) = output_receiver.recv().await {
                    match msg {
                        StreamingSinkWorkerMessage::Morsel {
                            input_id,
                            partition,
                        } => {
                            println!(
                                "[start-async] Received Morsel from worker for input_id {:?}, partition_len: {}",
                                input_id,
                                partition.len()
                            );
                            if counting_sender
                                .send(PipelineMessage::Morsel {
                                    input_id,
                                    partition,
                                })
                                .await
                                .is_err()
                            {
                                println!("[start-async] counting_sender failed to send morsel for input_id {:?}", input_id);
                                break;
                            }
                        }
                        StreamingSinkWorkerMessage::FlushState(input_id, state_opt) => {
                            println!(
                                "[start-async] Received FlushState from worker for input_id {:?} (state.is_some() = {})",
                                input_id,
                                state_opt.is_some()
                            );
                            // Received a flush state from a worker
                            // Only add to all_states if state exists
                            if let Some(state) = state_opt {
                                all_states
                                    .entry(input_id)
                                    .or_insert_with(Vec::new)
                                    .push(state);
                            }
                            let count = flush_counts.entry(input_id).or_insert(0);
                            *count += 1;
                            println!(
                                "[start-async] flush count for input_id {:?} now at {} / {}",
                                input_id, *count, num_workers_spawned
                            );

                            // Invariant: count should never exceed num_workers
                            // Each worker should send exactly one flush state per input_id
                            assert!(
                                *count <= num_workers_spawned,
                                "Flush count ({}) exceeded num_workers ({}) for input_id: {:?}",
                                *count,
                                num_workers_spawned,
                                input_id
                            );

                            // Only finalize when all workers have sent their flush states
                            if *count == num_workers_spawned {
                                let states = all_states.remove(&input_id).unwrap_or_default();
                                flush_counts.remove(&input_id);

                                println!(
                                    "[start-async] All workers flushed for input_id {:?}, finalizing ({} states)",
                                    input_id, states.len()
                                );
                                if !Self::finalize_and_send_output(
                                    op.clone(),
                                    states,
                                    input_id,
                                    memory_manager.clone(),
                                    runtime_stats.clone(),
                                    &counting_sender,
                                )
                                .await?
                                {
                                    println!("[start-async] finalize_and_send_output returned false, breaking");
                                    break;
                                }
                            }
                        }
                    }
                }

                // Finish up finalizing remaining states here.
                // At this point, there may still be input_ids that have not been fully flushed/finalized
                // because their workers exited early. For any such input_id, finalize the accumulated states.
                for (input_id, states) in all_states.drain() {
                    println!(
                        "[start-async] Finalizing remaining input_id {:?} ({} states)",
                        input_id, states.len()
                    );
                    if !Self::finalize_and_send_output(
                        op.clone(),
                        states,
                        input_id,
                        memory_manager.clone(),
                        runtime_stats.clone(),
                        &counting_sender,
                    )
                    .await?
                    {
                        println!("[start-async] finalize_and_send_output returned false for input_id {:?}", input_id);
                        break;
                    }
                }

                println!("[start-async] Finalizing stats for node_id {}", node_id);
                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &self.name(),
        );
        println!(
            "[start] StreamingSinkNode '{}' returning destination_receiver",
            self.name()
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
