use std::{sync::Arc, time::Instant};

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
use tracing::info_span;

use crate::{
    ControlFlow, ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender, create_channel},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    pipeline_execution::{InputStatesTracker, PipelineEvent, StateTracker, next_event},
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
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
    fn max_concurrency_per_input_id(&self) -> usize {
        get_compute_pool_num_threads()
    }
    fn total_max_concurrency(&self) -> usize {
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

/// Per-node task result for streaming sinks.
enum StreamingSinkTaskResult<S, Op: StreamingSink> {
    Execute {
        input_id: InputId,
        state: S,
        result: StreamingSinkOutput,
        elapsed: std::time::Duration,
    },
    Finalize {
        input_id: InputId,
        output: StreamingSinkFinalizeOutput<Op>,
    },
}

struct StreamingSinkProcessor<Op: StreamingSink> {
    task_set: OrderingAwareJoinSet<DaftResult<StreamingSinkTaskResult<Op::State, Op>>>,
    max_concurrency: usize,
    input_state_tracker: InputStatesTracker<Op::State>,
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    finalize_spawner: ExecutionTaskSpawner,
    runtime_stats: Arc<dyn RuntimeStats>,
    output_sender: Sender<PipelineMessage>,
    batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    stats_manager: RuntimeStatsManagerHandle,
    node_id: usize,
    node_initialized: bool,
}

impl<Op: StreamingSink + 'static> StreamingSinkProcessor<Op> {
    fn spawn_execute_task(
        &mut self,
        partition: Arc<MicroPartition>,
        state: Op::State,
        input_id: InputId,
    ) {
        let op = self.op.clone();
        let task_spawner = self.task_spawner.clone();
        self.task_set.spawn(async move {
            let now = Instant::now();
            let (new_state, result) = op.execute(partition, state, &task_spawner).await??;
            let elapsed = now.elapsed();
            Ok(StreamingSinkTaskResult::Execute {
                input_id,
                state: new_state,
                result,
                elapsed,
            })
        });
    }

    fn spawn_finalize_task(&mut self, states: Vec<Op::State>, input_id: InputId) {
        let op = self.op.clone();
        let finalize_spawner = self.finalize_spawner.clone();
        self.task_set.spawn(async move {
            let output = op.finalize(states, &finalize_spawner).await??;
            Ok(StreamingSinkTaskResult::Finalize { input_id, output })
        });
    }

    fn try_progress_input(&mut self, input_id: InputId) -> DaftResult<()> {
        while self.task_set.len() < self.max_concurrency
            && let Some(next) = self
                .input_state_tracker
                .get_next_morsel_for_execute(input_id)
        {
            let (partition, state) = next?;
            self.spawn_execute_task(partition, state, input_id);
        }

        if self.task_set.len() < self.max_concurrency
            && let Some(states) = self
                .input_state_tracker
                .try_take_states_for_finalize(input_id)
        {
            self.spawn_finalize_task(states, input_id);
        }
        Ok(())
    }

    fn try_progress_all_inputs(&mut self) -> DaftResult<()> {
        let input_ids = self.input_state_tracker.input_ids();
        for input_id in input_ids {
            self.try_progress_input(input_id)?;
        }
        Ok(())
    }

    async fn handle_execute_completed(
        &mut self,
        input_id: InputId,
        state: Op::State,
        result: StreamingSinkOutput,
        elapsed: std::time::Duration,
    ) -> DaftResult<ControlFlow> {
        self.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

        // Send output if present
        if let Some(mp) = result.output() {
            self.runtime_stats.add_rows_out(mp.len() as u64);
            self.batch_manager.record_execution_stats(
                self.runtime_stats.as_ref(),
                mp.len(),
                elapsed,
            );
            if self
                .output_sender
                .send(PipelineMessage::Morsel {
                    input_id,
                    partition: mp.clone(),
                })
                .await
                .is_err()
            {
                return Ok(ControlFlow::Stop);
            }
        }

        // Update batch bounds
        let new_requirements = self.batch_manager.calculate_batch_size();
        self.input_state_tracker.update_all_bounds(new_requirements);

        match result {
            StreamingSinkOutput::NeedMoreInput(_) => {
                self.input_state_tracker.return_state(input_id, state);
                self.try_progress_input(input_id)?;
                self.try_progress_all_inputs()?;
                Ok(ControlFlow::Continue)
            }
            StreamingSinkOutput::HasMoreOutput { input, .. } => {
                // Spawn another execution with same input and state
                let op = self.op.clone();
                let task_spawner = self.task_spawner.clone();
                self.task_set.spawn(async move {
                    let now = Instant::now();
                    let (new_state, result) = op.execute(input, state, &task_spawner).await??;
                    let elapsed = now.elapsed();
                    Ok(StreamingSinkTaskResult::Execute {
                        input_id,
                        state: new_state,
                        result,
                        elapsed,
                    })
                });
                Ok(ControlFlow::Continue)
            }
            StreamingSinkOutput::Finished(_) => {
                // Return state and flush all active input_ids, then stop
                self.input_state_tracker.return_state(input_id, state);
                let active_input_ids = self.input_state_tracker.input_ids();
                for flush_input_id in active_input_ids {
                    if self
                        .output_sender
                        .send(PipelineMessage::Flush(flush_input_id))
                        .await
                        .is_err()
                    {
                        return Ok(ControlFlow::Stop);
                    }
                }
                Ok(ControlFlow::Stop)
            }
        }
    }

    async fn handle_finalize_completed(
        &mut self,
        input_id: InputId,
        output: StreamingSinkFinalizeOutput<Op>,
    ) -> DaftResult<ControlFlow> {
        match output {
            StreamingSinkFinalizeOutput::HasMoreOutput { states, output } => {
                // Send finalize output if present
                if let Some(mp) = output {
                    self.runtime_stats.add_rows_out(mp.len() as u64);
                    if self
                        .output_sender
                        .send(PipelineMessage::Morsel {
                            input_id,
                            partition: mp,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(ControlFlow::Stop);
                    }
                }

                // Spawn another finalize task with the returned states
                self.spawn_finalize_task(states, input_id);
                Ok(ControlFlow::Continue)
            }
            StreamingSinkFinalizeOutput::Finished(output) => {
                // Send final output if present
                if let Some(mp) = output {
                    self.runtime_stats.add_rows_out(mp.len() as u64);
                    if self
                        .output_sender
                        .send(PipelineMessage::Morsel {
                            input_id,
                            partition: mp,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(ControlFlow::Stop);
                    }
                }

                // Finalization complete, send flush
                if self
                    .output_sender
                    .send(PipelineMessage::Flush(input_id))
                    .await
                    .is_err()
                {
                    return Ok(ControlFlow::Stop);
                }
                self.try_progress_all_inputs()?;
                Ok(ControlFlow::Continue)
            }
        }
    }

    fn handle_morsel(
        &mut self,
        input_id: InputId,
        partition: Arc<MicroPartition>,
    ) -> DaftResult<()> {
        if !self.node_initialized {
            self.stats_manager.activate_node(self.node_id);
            self.node_initialized = true;
        }
        self.runtime_stats.add_rows_in(partition.len() as u64);
        self.input_state_tracker
            .buffer_partition(input_id, partition)?;
        self.try_progress_input(input_id)?;
        Ok(())
    }

    async fn handle_flush(&mut self, input_id: InputId) -> DaftResult<ControlFlow> {
        if !self.input_state_tracker.contains_key(input_id) {
            if self
                .output_sender
                .send(PipelineMessage::Flush(input_id))
                .await
                .is_err()
            {
                return Ok(ControlFlow::Stop);
            }
            return Ok(ControlFlow::Continue);
        }
        self.input_state_tracker.mark_completed(input_id);
        self.try_progress_input(input_id)?;
        Ok(ControlFlow::Continue)
    }

    async fn handle_input_closed(&mut self) -> DaftResult<ControlFlow> {
        self.input_state_tracker.mark_all_completed();
        self.try_progress_all_inputs()?;
        Ok(ControlFlow::Continue)
    }

    async fn process_input(
        &mut self,
        receiver: &mut Receiver<PipelineMessage>,
    ) -> DaftResult<ControlFlow> {
        let mut input_closed = false;

        while !input_closed || !self.task_set.is_empty() || !self.input_state_tracker.is_empty() {
            let event = next_event(
                &mut self.task_set,
                self.max_concurrency,
                receiver,
                &mut input_closed,
            )
            .await?;
            let cf = match event {
                PipelineEvent::TaskCompleted(StreamingSinkTaskResult::Execute {
                    input_id,
                    state,
                    result,
                    elapsed,
                }) => {
                    self.handle_execute_completed(input_id, state, result, elapsed)
                        .await?
                }
                PipelineEvent::TaskCompleted(StreamingSinkTaskResult::Finalize {
                    input_id,
                    output,
                }) => self.handle_finalize_completed(input_id, output).await?,
                PipelineEvent::Morsel {
                    input_id,
                    partition,
                } => {
                    self.handle_morsel(input_id, partition)?;
                    ControlFlow::Continue
                }
                PipelineEvent::Flush(input_id) => self.handle_flush(input_id).await?,
                PipelineEvent::InputClosed => self.handle_input_closed().await?,
            };
            if cf == ControlFlow::Stop {
                return Ok(ControlFlow::Stop);
            }
        }

        self.stats_manager.finalize_node(self.node_id);
        Ok(ControlFlow::Continue)
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

        let op = self.op.clone();
        let input_state_tracker = InputStatesTracker::new(Box::new(
            move |_input_id| -> DaftResult<StateTracker<Op::State>> {
                let states = (0..op.max_concurrency_per_input_id())
                    .map(|_| op.make_state())
                    .collect::<DaftResult<Vec<_>>>()?;
                let (lower, upper) = op
                    .batching_strategy()
                    .calculate_new_requirements(&mut op.batching_strategy().make_state())
                    .values();
                Ok(StateTracker::new(states, RowBasedBuffer::new(lower, upper)))
            },
        ));

        let batch_manager = Arc::new(BatchManager::new(self.op.batching_strategy()));
        let max_concurrency = self.op.total_max_concurrency();

        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let node_id = self.node_id();
        let stats_manager = runtime_handle.stats_manager();
        runtime_handle.spawn(
            async move {
                let mut processor = StreamingSinkProcessor {
                    task_set: OrderingAwareJoinSet::new(maintain_order),
                    max_concurrency,
                    input_state_tracker,
                    op,
                    task_spawner,
                    finalize_spawner,
                    runtime_stats,
                    output_sender: destination_sender,
                    batch_manager,
                    stats_manager: stats_manager.clone(),
                    node_id,
                    node_initialized: false,
                };

                processor.process_input(&mut child_results_receiver).await?;

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
