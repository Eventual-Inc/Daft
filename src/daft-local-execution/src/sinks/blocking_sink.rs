use std::{
    collections::{HashMap, VecDeque, hash_map::Entry},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use common_checkpoint_config::CheckpointIdMap;
use common_display::tree::TreeDisplay;
use common_error::{DaftError, DaftResult};
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeInfo, NodeType},
};
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads, get_compute_runtime};
use daft_checkpoint::CheckpointStoreRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_memory::{MemoryPool, MemoryReleaseRequest, MemoryReleaseTarget};
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use futures::{Stream, StreamExt};
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    channel::{Receiver, Sender, create_channel},
    pipeline::{
        BuilderContext, InputId, MorselSizeRequirement, NodeName, PipelineEvent, PipelineMessage,
        PipelineNode, next_event,
    },
    resource_manager::PipelineMemoryContext,
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
    spilling::{SpillManager, SpillScopeId},
};

pub(crate) type BlockingSinkSinkResult<Op> =
    OperatorOutput<DaftResult<<Op as BlockingSink>::State>>;
pub(crate) enum BlockingSinkOutput {
    Partitions(Pin<Box<dyn Stream<Item = DaftResult<MicroPartition>> + Send>>),
    FlightPartitionRefs(Vec<FlightPartitionRef>),
}
impl BlockingSinkOutput {
    pub fn partitions(partitions: Vec<MicroPartition>) -> Self {
        Self::Partitions(Box::pin(futures::stream::iter(
            partitions.into_iter().map(Ok),
        )))
    }
}
pub(crate) type BlockingSinkFinalizeResult = OperatorOutput<DaftResult<BlockingSinkOutput>>;
pub(crate) type BlockingSinkReleaseResult<State> = OperatorOutput<DaftResult<(Vec<State>, u64)>>;

pub(crate) trait BlockingSink: Send + Sync {
    type State: Send + Sync + Unpin + 'static;
    type Stats: RuntimeStats = DefaultRuntimeStats;

    fn sink(
        &self,
        input: MicroPartition,
        state: Self::State,
        runtime_stats: Arc<Self::Stats>,
        spill_scope: SpillScopeId,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self>
    where
        Self: Sized;
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spill_scope: SpillScopeId,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult
    where
        Self: Sized;
    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State>;
    fn reclaim_bytes(&self, _states: &[Self::State]) -> u64 {
        0
    }
    fn release_memory(
        &self,
        states: Vec<Self::State>,
        _target_bytes: u64,
        _spill_scope: SpillScopeId,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkReleaseResult<Self::State>
    where
        Self: Sized,
    {
        Ok((states, 0)).into()
    }
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
}

enum TaskResult<Op: BlockingSink> {
    Sink(InputId, Op::State, Duration),
    MemoryReleased(InputId, Vec<Op::State>, MemoryReleaseRequest, u64),
    Finalized,
}

enum BlockingSinkEvent<Op: BlockingSink> {
    Pipeline(PipelineEvent<TaskResult<Op>>),
    Release(MemoryReleaseRequest),
}

async fn next_blocking_sink_event<Op: BlockingSink + 'static>(
    tasks: &mut OrderingAwareJoinSet<DaftResult<TaskResult<Op>>>,
    max_concurrency: usize,
    child_rx: &mut Receiver<PipelineMessage>,
    child_closed: &mut bool,
    release_target: &mut MemoryReleaseTarget,
) -> DaftResult<Option<BlockingSinkEvent<Op>>> {
    tokio::select! {
        event = next_event(tasks, max_concurrency, child_rx, child_closed) => {
            Ok(event?.map(BlockingSinkEvent::Pipeline))
        }
        request = release_target.recv() => Ok(Some(BlockingSinkEvent::Release(request))),
    }
}

struct PerInputState<Op: BlockingSink> {
    states: Vec<Op::State>,
    pending: VecDeque<MicroPartition>,
    max_concurrency: usize,
    flushed: bool,
    runtime_stats: Arc<Op::Stats>,
    memory_pool: Arc<MemoryPool>,
    spill_scope: SpillScopeId,
}

impl<Op: BlockingSink + 'static> PerInputState<Op> {
    fn new(
        op: &Arc<Op>,
        max_concurrency: usize,
        runtime_stats: Arc<Op::Stats>,
        input_id: InputId,
        memory_pool: Arc<MemoryPool>,
        spill_scope: SpillScopeId,
    ) -> DaftResult<Self> {
        let states = (0..max_concurrency)
            .map(|_| op.make_state(input_id))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            states,
            pending: VecDeque::new(),
            max_concurrency,
            flushed: false,
            runtime_stats,
            memory_pool,
            spill_scope,
        })
    }

    fn spawn_sink(
        &mut self,
        tasks: &mut OrderingAwareJoinSet<DaftResult<TaskResult<Op>>>,
        op: &Arc<Op>,
        spawner: &ExecutionTaskSpawner,
        input_id: InputId,
        partition: MicroPartition,
    ) {
        let Some(state) = self.states.pop() else {
            return;
        };
        let op = op.clone();
        let spawner = spawner.with_memory_pool(self.memory_pool.clone());
        let spill_scope = self.spill_scope.clone();
        let runtime_stats = self.runtime_stats.clone();
        tasks.spawn(async move {
            let now = Instant::now();
            let state = op
                .sink(partition, state, runtime_stats, spill_scope, &spawner)
                .await??;
            Ok(TaskResult::Sink(input_id, state, now.elapsed()))
        });
    }

    fn flush_pending(
        &mut self,
        tasks: &mut OrderingAwareJoinSet<DaftResult<TaskResult<Op>>>,
        op: &Arc<Op>,
        spawner: &ExecutionTaskSpawner,
        input_id: InputId,
    ) -> DaftResult<()> {
        if self.pending.is_empty() || self.states.is_empty() {
            return Ok(());
        }
        let parts: Vec<_> = self.pending.drain(..).collect();
        let partition = MicroPartition::concat(parts)?;
        self.spawn_sink(tasks, op, spawner, input_id, partition);
        Ok(())
    }

    fn all_states_idle(&self) -> bool {
        self.states.len() == self.max_concurrency
    }

    fn ready_to_finalize(&self) -> bool {
        self.flushed && self.all_states_idle()
    }

    fn reclaim_bytes(&self, op: &Op) -> u64 {
        op.reclaim_bytes(&self.states)
    }
}

pub struct BlockingSinkNode<Op: BlockingSink> {
    op: Arc<Op>,
    child: Box<dyn PipelineNode>,
    meter: Meter,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
    checkpoint: Option<(
        CheckpointStoreRef,
        CheckpointIdMap,
        daft_checkpoint::FileFormat,
    )>,
}

impl<Op: BlockingSink + 'static> BlockingSinkNode<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        child: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = op.name().into();
        let node_info = ctx.next_node_info(name, op.op_type(), NodeCategory::BlockingSink, context);
        Self {
            op,
            child,
            meter: ctx.meter.clone(),
            plan_stats,
            node_info: Arc::new(node_info),
            checkpoint: None,
        }
    }

    /// Set the checkpoint context for this sink node. When present, the store's
    /// `checkpoint()` method is called after each input finalizes, using a
    /// per-input `CheckpointId` derived from the shared `CheckpointIdMap`.
    pub(crate) fn with_checkpoint(
        mut self,
        store: CheckpointStoreRef,
        id_map: CheckpointIdMap,
        file_format: daft_checkpoint::FileFormat,
    ) -> Self {
        self.checkpoint = Some((store, id_map, file_format));
        self
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    fn encode_file_metadata(
        partitions: &[MicroPartition],
        file_format: daft_checkpoint::FileFormat,
    ) -> DaftResult<Vec<daft_checkpoint::FileMetadata>> {
        let ipc_err = |e: arrow_schema::ArrowError| DaftError::InternalError(e.to_string());
        partitions
            .iter()
            .flat_map(|mp| {
                mp.record_batches().iter().map(move |rb| {
                    let mut buf = Vec::new();
                    let schema = rb.schema.to_arrow()?;
                    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema)
                        .map_err(ipc_err)?;
                    let arrow_arrays: Vec<arrow_array::ArrayRef> = rb
                        .columns()
                        .iter()
                        .map(|c| c.as_materialized_series().to_arrow())
                        .collect::<DaftResult<_>>()?;
                    let batch = arrow_array::RecordBatch::try_new(
                        std::sync::Arc::new(schema),
                        arrow_arrays,
                    )
                    .map_err(ipc_err)?;
                    writer.write(&batch).map_err(ipc_err)?;
                    writer.finish().map_err(ipc_err)?;
                    drop(writer);
                    Ok(daft_checkpoint::FileMetadata::new(file_format, buf))
                })
            })
            .collect()
    }

    fn spawn_finalize(
        op: Arc<Op>,
        per_input: PerInputState<Op>,
        input_id: InputId,
        finalize_spawner: ExecutionTaskSpawner,
        output_tx: Sender<PipelineMessage>,
        tasks: &mut OrderingAwareJoinSet<DaftResult<TaskResult<Op>>>,
        checkpoint: Option<(
            CheckpointStoreRef,
            CheckpointIdMap,
            daft_checkpoint::FileFormat,
        )>,
    ) {
        let finalize_spawner = finalize_spawner.with_memory_pool(per_input.memory_pool.clone());
        tasks.spawn(async move {
            let checkpoint_id = checkpoint
                .as_ref()
                .map(|(_, id_map, _)| id_map.get_or_generate(input_id));

            let output = op
                .finalize(per_input.states, per_input.spill_scope, &finalize_spawner)
                .await??;
            per_input.runtime_stats.increment_num_tasks();
            match output {
                BlockingSinkOutput::Partitions(mut partitions) => {
                    while let Some(partition) = partitions.next().await {
                        let partition = partition?;
                        // Stage each output before forwarding it, preserving checkpoint ordering
                        // without collecting the whole output stream in memory.
                        if let Some((ref store, _, file_format)) = checkpoint {
                            let file_metadata = Self::encode_file_metadata(
                                std::slice::from_ref(&partition),
                                file_format,
                            )?;
                            if !file_metadata.is_empty() {
                                let num_files = file_metadata.len() as u64;
                                store
                                    .stage_files(checkpoint_id.as_ref().unwrap(), file_metadata)
                                    .await?;
                                per_input
                                    .runtime_stats
                                    .add_checkpoint_files_staged(num_files);
                            }
                        }
                        per_input.runtime_stats.add_rows_out(partition.len() as u64);
                        per_input
                            .runtime_stats
                            .add_bytes_out(partition.size_bytes() as u64);
                        if output_tx
                            .send(PipelineMessage::Morsel {
                                input_id,
                                partition,
                            })
                            .await
                            .is_err()
                        {
                            return Ok(TaskResult::Finalized);
                        }
                    }
                }
                BlockingSinkOutput::FlightPartitionRefs(partition_refs) => {
                    for partition_ref in partition_refs {
                        if output_tx
                            .send(PipelineMessage::FlightPartitionRef {
                                input_id,
                                partition_ref,
                            })
                            .await
                            .is_err()
                        {
                            return Ok(TaskResult::Finalized);
                        }
                    }
                }
            }
            if let Some((store, _, _)) = &checkpoint {
                store.checkpoint(checkpoint_id.as_ref().unwrap()).await?;
                per_input.runtime_stats.add_checkpoints_sealed(1);
            }
            let _ = output_tx.send(PipelineMessage::Flush(input_id)).await;
            Ok(TaskResult::Finalized)
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn run(
        op: Arc<Op>,
        mut child_rx: Receiver<PipelineMessage>,
        output_tx: Sender<PipelineMessage>,
        memory_pool: Arc<MemoryPool>,
        memory_context: Arc<PipelineMemoryContext>,
        spill_manager: SpillManager,
        stats_manager: RuntimeStatsManagerHandle,
        meter: Meter,
        node_info: Arc<NodeInfo>,
        checkpoint: Option<(
            CheckpointStoreRef,
            CheckpointIdMap,
            daft_checkpoint::FileFormat,
        )>,
    ) -> DaftResult<()> {
        let node_id = node_info.id;
        let max_concurrency = op.max_concurrency();
        let compute_runtime = get_compute_runtime();
        let mut release_target = memory_pool
            .register_release_target(format!("blocking-node-{node_id}-{}", node_info.name));
        let task_spawner = ExecutionTaskSpawner::new(
            compute_runtime.clone(),
            memory_pool.clone(),
            spill_manager.clone(),
            info_span!("BlockingSink::Sink"),
        );
        let finalize_spawner = ExecutionTaskSpawner::new(
            compute_runtime,
            memory_pool,
            spill_manager.clone(),
            info_span!("BlockingSink::Finalize"),
        );
        let mut inputs: HashMap<InputId, PerInputState<Op>> = HashMap::new();
        let mut tasks: OrderingAwareJoinSet<DaftResult<TaskResult<Op>>> =
            OrderingAwareJoinSet::new(false);
        let mut child_closed = false;
        let mut node_initialized = false;

        while let Some(event) = next_blocking_sink_event(
            &mut tasks,
            max_concurrency,
            &mut child_rx,
            &mut child_closed,
            &mut release_target,
        )
        .await?
        {
            match event {
                BlockingSinkEvent::Pipeline(PipelineEvent::TaskCompleted(TaskResult::Sink(
                    input_id,
                    state,
                    elapsed,
                ))) => {
                    let per_input = inputs.get_mut(&input_id).unwrap();
                    per_input
                        .runtime_stats
                        .add_duration_us(elapsed.as_micros() as u64);
                    per_input.runtime_stats.increment_num_tasks();
                    per_input.states.push(state);
                    per_input.flush_pending(&mut tasks, &op, &task_spawner, input_id)?;

                    if per_input.ready_to_finalize() {
                        Self::spawn_finalize(
                            op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            finalize_spawner.clone(),
                            output_tx.clone(),
                            &mut tasks,
                            checkpoint.clone(),
                        );
                    }
                }
                BlockingSinkEvent::Pipeline(PipelineEvent::TaskCompleted(
                    TaskResult::MemoryReleased(input_id, states, request, released_bytes),
                )) => {
                    let ready_to_finalize = if let Some(per_input) = inputs.get_mut(&input_id) {
                        // Sink tasks can return other states while this release task is running.
                        // Preserve those states when returning the reclaimed subset.
                        per_input.states.extend(states);
                        per_input.flush_pending(&mut tasks, &op, &task_spawner, input_id)?;
                        per_input.ready_to_finalize()
                    } else {
                        false
                    };
                    request.complete(released_bytes);
                    if ready_to_finalize {
                        Self::spawn_finalize(
                            op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            finalize_spawner.clone(),
                            output_tx.clone(),
                            &mut tasks,
                            checkpoint.clone(),
                        );
                    }
                }
                BlockingSinkEvent::Pipeline(PipelineEvent::TaskCompleted(
                    TaskResult::Finalized,
                )) => {}
                BlockingSinkEvent::Pipeline(PipelineEvent::Morsel {
                    input_id,
                    partition,
                }) => {
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
                                input_id,
                                memory_context.operator_pool(input_id, node_id, &node_info.name),
                                spill_manager.scope(input_id, node_id),
                            )?)
                        }
                    };
                    per_input.runtime_stats.add_rows_in(partition.len() as u64);
                    per_input
                        .runtime_stats
                        .add_bytes_in(partition.size_bytes() as u64);
                    per_input.pending.push_back(partition);
                    per_input.flush_pending(&mut tasks, &op, &task_spawner, input_id)?;
                }
                BlockingSinkEvent::Pipeline(PipelineEvent::FlightPartitionRef) => {
                    unreachable!(
                        "BlockingSinkNode should not receive flight partition refs from child"
                    )
                }
                BlockingSinkEvent::Pipeline(PipelineEvent::Flush(input_id)) => {
                    // A Flush can arrive for an input that received zero morsels (e.g.
                    // an empty source after the checkpoint anti-join). Without a
                    // PerInputState the flush was silently dropped and finalize never
                    // ran, leaving the checkpoint unsealed. Create one on demand,
                    // mirroring the Vacant arm in the partition handler above.
                    if let Entry::Vacant(e) = inputs.entry(input_id) {
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
                            input_id,
                            memory_context.operator_pool(input_id, node_id, &node_info.name),
                            spill_manager.scope(input_id, node_id),
                        )?);
                    }
                    inputs.get_mut(&input_id).unwrap().flushed = true;
                    if inputs[&input_id].ready_to_finalize() {
                        Self::spawn_finalize(
                            op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            finalize_spawner.clone(),
                            output_tx.clone(),
                            &mut tasks,
                            checkpoint.clone(),
                        );
                    }
                }
                BlockingSinkEvent::Pipeline(PipelineEvent::InputClosed) => {
                    for per_input in inputs.values_mut() {
                        per_input.flushed = true;
                    }
                    let ready: Vec<_> = inputs
                        .keys()
                        .filter(|id| inputs[id].ready_to_finalize())
                        .copied()
                        .collect();
                    for input_id in ready {
                        Self::spawn_finalize(
                            op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            finalize_spawner.clone(),
                            output_tx.clone(),
                            &mut tasks,
                            checkpoint.clone(),
                        );
                    }
                }
                BlockingSinkEvent::Release(request) => {
                    let candidate = inputs
                        .iter()
                        .filter_map(|(input_id, per_input)| {
                            let bytes = per_input.reclaim_bytes(op.as_ref());
                            (bytes > 0).then_some((*input_id, bytes))
                        })
                        .max_by_key(|(_, bytes)| *bytes);
                    if let Some((input_id, _)) = candidate {
                        let per_input = inputs.get_mut(&input_id).unwrap();
                        let states = std::mem::take(&mut per_input.states);
                        let op = op.clone();
                        let spawner = task_spawner.with_memory_pool(per_input.memory_pool.clone());
                        let target_bytes = request.target_bytes();
                        let spill_scope = per_input.spill_scope.clone();
                        tasks.spawn(async move {
                            let (states, released_bytes) = op
                                .release_memory(states, target_bytes, spill_scope, &spawner)
                                .await??;
                            Ok(TaskResult::MemoryReleased(
                                input_id,
                                states,
                                request,
                                released_bytes,
                            ))
                        });
                    } else {
                        request.complete(0);
                    }
                }
            }

            let reclaim_bytes = inputs
                .values()
                .map(|per_input| per_input.reclaim_bytes(op.as_ref()))
                .sum();
            release_target.set_reclaim_bytes(reclaim_bytes);
        }

        stats_manager.finalize_node(node_id);
        Ok(())
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
            _ => {
                let multiline_display = self.op.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
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

        let mut json = serde_json::json!({
            "id": self.node_id(),
            "category": "BlockingSink",
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
        self: Box<Self>,
        _maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let Self {
            op,
            child,
            meter,
            node_info,
            checkpoint,
            ..
        } = *self;
        let name: Arc<str> = node_info.name.clone();
        let child_rx = child.start(false, runtime_handle)?;
        let (output_tx, output_rx) = create_channel(1);
        let memory_pool = runtime_handle.memory_pool();
        let memory_context = runtime_handle.memory_context();
        let spill_manager = runtime_handle.spill_manager();
        memory_context.node_shared_pool(node_info.id, &node_info.name);
        let stats_manager = runtime_handle.stats_manager();

        runtime_handle.spawn(
            async move {
                Self::run(
                    op,
                    child_rx,
                    output_tx,
                    memory_pool,
                    memory_context,
                    spill_manager,
                    stats_manager,
                    meter,
                    node_info,
                    checkpoint,
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
