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
use common_runtime::{JoinSet, get_compute_pool_num_threads, get_compute_runtime};
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender, create_channel},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats},
};

/// Result of executing a streaming sink on a single partition.
#[derive(Debug)]
pub(crate) enum StreamingSinkOutput {
    /// Operator needs more input to continue processing.
    NeedMoreInput(Option<Arc<MicroPartition>>),

    /// Operator has more output to produce using the same input.
    #[allow(dead_code)]
    HasMoreOutput {
        input: Arc<MicroPartition>,
        output: Option<Arc<MicroPartition>>,
    },

    /// Operator is finished processing.
    Finished(Option<Arc<MicroPartition>>),
}

/// Result of finalizing a streaming sink.
pub(crate) enum StreamingSinkFinalizeOutput<Op: StreamingSink> {
    /// More finalization work is needed.
    HasMoreOutput {
        states: Vec<Op::State>,
        output: Option<Arc<MicroPartition>>,
    },
    /// Finalization is complete.
    Finished(Option<Arc<MicroPartition>>),
}

pub(crate) type StreamingSinkExecuteResult<Op> =
    OperatorOutput<DaftResult<(<Op as StreamingSink>::State, StreamingSinkOutput)>>;
pub(crate) type StreamingSinkFinalizeResult<Op> =
    OperatorOutput<DaftResult<StreamingSinkFinalizeOutput<Op>>>;
pub(crate) trait StreamingSink: Send + Sync {
    type State: Send + Sync + Unpin;
    type BatchingStrategy: BatchingStrategy + 'static;

    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
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
    fn make_state(&self) -> DaftResult<Self::State>;
    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(DefaultRuntimeStats::new(id))
    }
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }
    fn batching_strategy(&self) -> Self::BatchingStrategy;
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

/// Process all morsels for a single input_id, finalize, and send output.
#[allow(clippy::too_many_arguments)]
async fn process_single_input<Op: StreamingSink + 'static>(
    input_id: InputId,
    mut receiver: Receiver<PipelineMessage>,
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    finalize_spawner: ExecutionTaskSpawner,
    runtime_stats: Arc<dyn RuntimeStats>,
    output_sender: Sender<PipelineMessage>,
    batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    _maintain_order: bool,
) -> DaftResult<ControlFlow<()>> {
    let mut states: Vec<Op::State> = (0..op.max_concurrency())
        .map(|_| op.make_state())
        .collect::<DaftResult<_>>()?;

    let (lower, upper) = batch_manager.calculate_batch_size().values();
    let mut buffer = RowBasedBuffer::new(lower, upper);

    let mut task_set: JoinSet<DaftResult<(Op::State, StreamingSinkOutput, Duration)>> =
        JoinSet::new();
    let mut input_closed = false;
    let mut finished = false;

    while !finished && (!input_closed || !task_set.is_empty()) {
        // Try to spawn from buffer while states are available
        while !states.is_empty() {
            let batch = buffer.next_batch_if_ready()?;
            if let Some(partition) = batch {
                let state = states.pop().unwrap();
                let op = op.clone();
                let task_spawner = task_spawner.clone();
                task_set.spawn(async move {
                    let now = Instant::now();
                    let (new_state, result) = op.execute(partition, state, &task_spawner).await??;
                    Ok((new_state, result, now.elapsed()))
                });
            } else {
                break;
            }
        }

        if input_closed && task_set.is_empty() {
            break;
        }

        tokio::select! {
            biased;

            Some(result) = task_set.join_next(), if !task_set.is_empty() => {
                let (state, result, elapsed) = result??;
                runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

                // Send output if present
                if let Some(mp) = result.output() {
                    runtime_stats.add_rows_out(mp.len() as u64);
                    batch_manager.record_execution_stats(
                        runtime_stats.as_ref(),
                        mp.len(),
                        elapsed,
                    );
                    if output_sender
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

                // Update batch bounds
                let new_requirements = batch_manager.calculate_batch_size();
                buffer.update_bounds(new_requirements);

                match result {
                    StreamingSinkOutput::NeedMoreInput(_) => {
                        states.push(state);
                    }
                    StreamingSinkOutput::HasMoreOutput { input, .. } => {
                        let op = op.clone();
                        let task_spawner = task_spawner.clone();
                        task_set.spawn(async move {
                            let now = Instant::now();
                            let (new_state, result) =
                                op.execute(input, state, &task_spawner).await??;
                            Ok((new_state, result, now.elapsed()))
                        });
                    }
                    StreamingSinkOutput::Finished(_) => {
                        finished = true;
                    }
                }
            }

            msg = receiver.recv(), if !states.is_empty() && !input_closed => {
                match msg {
                    Some(PipelineMessage::Morsel { partition, .. }) => {
                        runtime_stats.add_rows_in(partition.len() as u64);
                        buffer.push(partition);
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
            let mut state = states.pop().unwrap();
            loop {
                let now = Instant::now();
                let (new_state, result) = op.execute(partition, state, &task_spawner).await??;
                let elapsed = now.elapsed();
                runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

                if let Some(mp) = result.output() {
                    runtime_stats.add_rows_out(mp.len() as u64);
                    batch_manager.record_execution_stats(runtime_stats.as_ref(), mp.len(), elapsed);
                    if output_sender
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

                let new_requirements = batch_manager.calculate_batch_size();
                buffer.update_bounds(new_requirements);

                match result {
                    StreamingSinkOutput::NeedMoreInput(_) => {
                        states.push(new_state);
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
        loop {
            match op.finalize(states, &finalize_spawner).await?? {
                StreamingSinkFinalizeOutput::HasMoreOutput {
                    states: new_states,
                    output,
                } => {
                    if let Some(mp) = output {
                        runtime_stats.add_rows_out(mp.len() as u64);
                        if output_sender
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
                    states = new_states;
                }
                StreamingSinkFinalizeOutput::Finished(output) => {
                    if let Some(mp) = output {
                        runtime_stats.add_rows_out(mp.len() as u64);
                        if output_sender
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
                    break;
                }
            }
        }
    }

    // Send flush
    if output_sender
        .send(PipelineMessage::Flush(input_id))
        .await
        .is_err()
    {
        return Ok(ControlFlow::Break(()));
    }
    Ok(ControlFlow::Continue(()))
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
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = op.name().into();
        let node_info =
            ctx.next_node_info(name, op.op_type(), NodeCategory::StreamingSink, context);
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
                let mut processor_set: JoinSet<DaftResult<ControlFlow<()>>> = JoinSet::new();
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

                            if !per_input_senders.contains_key(&input_id) {
                                let (tx, rx) = create_channel(1);
                                per_input_senders.insert(input_id, tx);

                                let op = op.clone();
                                let task_spawner = task_spawner.clone();
                                let finalize_spawner = finalize_spawner.clone();
                                let runtime_stats = runtime_stats.clone();
                                let output_sender = destination_sender.clone();
                                let batch_manager = batch_manager.clone();
                                processor_set.spawn(async move {
                                    process_single_input(
                                        input_id, rx, op, task_spawner,
                                        finalize_spawner, runtime_stats, output_sender,
                                        batch_manager, maintain_order,
                                    )
                                    .await
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
