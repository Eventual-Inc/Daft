use std::{
    collections::{HashMap, hash_map::Entry},
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
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender, create_channel},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::{
        BuilderContext, InputId, MorselSizeRequirement, NodeName, PipelineEvent, PipelineMessage,
        PipelineNode, next_event,
    },
    resource_manager::MemoryManager,
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
};

pub enum StreamingSinkOutput {
    NeedMoreInput(Option<MicroPartition>),
    Finished(Option<MicroPartition>),
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
    fn make_state(&self) -> DaftResult<Self::State>;
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }
    fn batching_strategy(&self) -> Self::BatchingStrategy;
}

enum TaskResult<Op: StreamingSink> {
    Execute(InputId, Op::State, StreamingSinkOutput, Duration),
    Completed,
}

struct PerInputState<Op: StreamingSink> {
    states: Vec<Op::State>,
    buffer: RowBasedBuffer,
    flushed: bool,
    max_concurrency: usize,
    runtime_stats: Arc<Op::Stats>,
    batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
}

impl<Op: StreamingSink + 'static> PerInputState<Op> {
    fn new(
        op: &Arc<Op>,
        max_concurrency: usize,
        runtime_stats: Arc<Op::Stats>,
        batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    ) -> DaftResult<Self> {
        let states = (0..max_concurrency)
            .map(|_| op.make_state())
            .collect::<Result<_, _>>()?;
        let (lower, upper) = batch_manager.calculate_batch_size().values();
        Ok(Self {
            states,
            buffer: RowBasedBuffer::new(lower, upper),
            flushed: false,
            max_concurrency,
            runtime_stats,
            batch_manager,
        })
    }

    fn spawn_ready_batches(
        &mut self,
        tasks: &mut OrderingAwareJoinSet<DaftResult<TaskResult<Op>>>,
        op: &Arc<Op>,
        spawner: &ExecutionTaskSpawner,
        input_id: InputId,
    ) -> DaftResult<()> {
        while let Some(state) = self.states.pop() {
            if let Some(batch) = self.buffer.next_batch_if_ready()? {
                let op = op.clone();
                let spawner = spawner.clone();
                let runtime_stats = self.runtime_stats.clone();
                tasks.spawn(async move {
                    let now = Instant::now();
                    let (state, output) =
                        op.execute(batch, state, runtime_stats, &spawner).await??;
                    Ok(TaskResult::Execute(input_id, state, output, now.elapsed()))
                });
            } else {
                self.states.push(state);
                break;
            }
        }
        Ok(())
    }

    fn all_states_idle(&self) -> bool {
        self.states.len() == self.max_concurrency
    }

    fn ready_to_complete(&self) -> bool {
        self.flushed && self.all_states_idle()
    }
}

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

    /// Drain remaining buffer, finalize, and send output downstream.
    fn spawn_complete_input(
        op: Arc<Op>,
        per_input: PerInputState<Op>,
        input_id: InputId,
        task_spawner: ExecutionTaskSpawner,
        finalize_spawner: ExecutionTaskSpawner,
        output_tx: Sender<PipelineMessage>,
        tasks: &mut OrderingAwareJoinSet<DaftResult<TaskResult<Op>>>,
    ) {
        tasks.spawn(async move {
            let PerInputState {
                mut states,
                mut buffer,
                runtime_stats,
                batch_manager,
                ..
            } = per_input;

            if let Some(partition) = buffer.pop_all()? {
                let state = states.pop().expect("must have at least one idle state");
                let now = Instant::now();
                let (new_state, result) = op
                    .execute(partition, state, runtime_stats.clone(), &task_spawner)
                    .await??;
                let elapsed = now.elapsed();
                runtime_stats.add_duration_us(elapsed.as_micros() as u64);

                let (mp, finished) = match result {
                    StreamingSinkOutput::NeedMoreInput(mp) => (mp, false),
                    StreamingSinkOutput::Finished(mp) => (mp, true),
                };
                batch_manager.record_execution_stats(
                    runtime_stats.as_ref(),
                    mp.as_ref().map(|p| p.len()).unwrap_or(0),
                    elapsed,
                );
                if let Some(mp) = mp {
                    runtime_stats.add_rows_out(mp.len() as u64);
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

    #[allow(clippy::too_many_arguments)]
    async fn run(
        op: Arc<Op>,
        mut child_rx: Receiver<PipelineMessage>,
        output_tx: Sender<PipelineMessage>,
        memory_manager: Arc<MemoryManager>,
        stats_manager: RuntimeStatsManagerHandle,
        meter: Meter,
        node_info: Arc<NodeInfo>,
        batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
        maintain_order: bool,
    ) -> DaftResult<()> {
        let node_id = node_info.id;
        let max_concurrency = op.max_concurrency();
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
        let mut inputs: HashMap<InputId, PerInputState<Op>> = HashMap::new();
        let mut tasks: OrderingAwareJoinSet<DaftResult<TaskResult<Op>>> =
            OrderingAwareJoinSet::new(maintain_order);
        let mut child_closed = false;
        let mut node_initialized = false;

        let op_name = op.name();
        while let Some(event) = next_event(
            &mut tasks,
            max_concurrency,
            &mut child_rx,
            &mut child_closed,
        )
        .await?
        {
            let per_input_info: Vec<_> = inputs
                .iter()
                .map(|(id, s)| {
                    format!(
                        "{}:{}active/{}pending",
                        id,
                        s.max_concurrency - s.states.len(),
                        s.buffer.is_empty()
                    )
                })
                .collect();
            println!(
                "[StreamingSink::{op_name}] total_tasks={} inputs={} [{per_input_info:?}]",
                tasks.len(),
                inputs.len(),
            );
            match event {
                PipelineEvent::TaskCompleted(TaskResult::Completed) => {}
                PipelineEvent::TaskCompleted(TaskResult::Execute(
                    input_id,
                    state,
                    output,
                    elapsed,
                )) => {
                    let per_input = inputs.get_mut(&input_id).unwrap();
                    per_input
                        .runtime_stats
                        .add_duration_us(elapsed.as_micros() as u64);

                    let (mp, finished) = match output {
                        StreamingSinkOutput::NeedMoreInput(mp) => (mp, false),
                        StreamingSinkOutput::Finished(mp) => (mp, true),
                    };
                    per_input.batch_manager.record_execution_stats(
                        per_input.runtime_stats.as_ref(),
                        mp.as_ref().map(|p| p.len()).unwrap_or(0),
                        elapsed,
                    );
                    if let Some(mp) = mp {
                        per_input.runtime_stats.add_rows_out(mp.len() as u64);
                        let _ = output_tx
                            .send(PipelineMessage::Morsel {
                                input_id,
                                partition: mp,
                            })
                            .await;
                    }

                    if finished {
                        // Operator signaled completion — flush and stop everything.
                        let _ = output_tx.send(PipelineMessage::Flush(input_id)).await;
                        break;
                    }

                    per_input.states.push(state);
                    let new_requirements = per_input.batch_manager.calculate_batch_size();
                    per_input.buffer.update_bounds(new_requirements);
                    per_input.spawn_ready_batches(&mut tasks, &op, &task_spawner, input_id)?;

                    if inputs.get(&input_id).is_some_and(|p| p.ready_to_complete()) {
                        Self::spawn_complete_input(
                            op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            task_spawner.clone(),
                            finalize_spawner.clone(),
                            output_tx.clone(),
                            &mut tasks,
                        );
                    }
                }
                PipelineEvent::Morsel {
                    input_id,
                    partition,
                } => {
                    if !node_initialized {
                        stats_manager.activate_node(node_id);
                        node_initialized = true;
                    }

                    let per_input = match inputs.entry(input_id) {
                        Entry::Occupied(e) => e.into_mut(),
                        Entry::Vacant(e) => {
                            let runtime_stats = Arc::new(Op::Stats::new(&meter, &node_info));
                            stats_manager.register_runtime_stats(
                                node_id,
                                input_id,
                                runtime_stats.clone(),
                            );
                            e.insert(PerInputState::new(
                                &op,
                                max_concurrency,
                                runtime_stats,
                                batch_manager.clone(),
                            )?)
                        }
                    };
                    per_input.runtime_stats.add_rows_in(partition.len() as u64);
                    per_input.buffer.push(partition);
                    per_input.spawn_ready_batches(&mut tasks, &op, &task_spawner, input_id)?;
                }
                PipelineEvent::Flush(input_id) => {
                    if !node_initialized {
                        stats_manager.activate_node(node_id);
                        node_initialized = true;
                    }

                    if let Some(p) = inputs.get_mut(&input_id) {
                        p.flushed = true;
                    }
                    if inputs.get(&input_id).is_some_and(|p| p.ready_to_complete()) {
                        Self::spawn_complete_input(
                            op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            task_spawner.clone(),
                            finalize_spawner.clone(),
                            output_tx.clone(),
                            &mut tasks,
                        );
                    }
                }
                PipelineEvent::InputClosed => {
                    for p in inputs.values_mut() {
                        p.flushed = true;
                    }
                    let ready_ids: Vec<_> = inputs
                        .iter()
                        .filter(|(_, p)| p.ready_to_complete())
                        .map(|(id, _)| *id)
                        .collect();
                    for input_id in ready_ids {
                        Self::spawn_complete_input(
                            op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            task_spawner.clone(),
                            finalize_spawner.clone(),
                            output_tx.clone(),
                            &mut tasks,
                        );
                    }
                }
            }
        }

        stats_manager.finalize_node(node_id);
        Ok(())
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
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let Self {
            op,
            child,
            meter,
            node_info,
            ..
        } = *self;
        let name: Arc<str> = node_info.name.clone();
        let child_rx = child.start(maintain_order, runtime_handle)?;
        let (output_tx, output_rx) = create_channel(1);
        let memory_manager = runtime_handle.memory_manager();
        let stats_manager = runtime_handle.stats_manager();
        let batch_manager = Arc::new(BatchManager::new(op.batching_strategy()));

        runtime_handle.spawn(
            async move {
                Self::run(
                    op,
                    child_rx,
                    output_tx,
                    memory_manager,
                    stats_manager,
                    meter,
                    node_info,
                    batch_manager,
                    maintain_order,
                )
                .await
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
