use std::{
    collections::{HashMap, VecDeque, hash_map::Entry},
    sync::Arc,
    time::{Duration, Instant},
};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeInfo, NodeType},
};
use common_runtime::{
    JoinSet, OrderingAwareJoinSet, get_compute_pool_num_threads, get_compute_runtime,
};
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    channel::{Receiver, Sender, create_channel},
    pipeline::{
        BuilderContext, InputId, MorselSizeRequirement, NodeName, PipelineEvent, PipelineMessage,
        PipelineNode, next_event,
    },
    resource_manager::MemoryManager,
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
};

pub(crate) type BlockingSinkSinkResult<Op> =
    OperatorOutput<DaftResult<<Op as BlockingSink>::State>>;
pub(crate) type BlockingSinkFinalizeResult = OperatorOutput<DaftResult<Vec<MicroPartition>>>;

pub(crate) trait BlockingSink: Send + Sync {
    type State: Send + Sync + Unpin;
    type Stats: RuntimeStats = DefaultRuntimeStats;

    fn sink(
        &self,
        input: MicroPartition,
        state: Self::State,
        runtime_stats: Arc<Self::Stats>,
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
    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State>;
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
}

type SinkTaskResult<Op> = DaftResult<(InputId, <Op as BlockingSink>::State, Duration)>;

struct PerInputState<Op: BlockingSink> {
    states: Vec<Op::State>,
    pending: VecDeque<MicroPartition>,
    max_concurrency: usize,
    flushed: bool,
    runtime_stats: Arc<Op::Stats>,
}

impl<Op: BlockingSink + 'static> PerInputState<Op> {
    fn new(
        op: &Arc<Op>,
        max_concurrency: usize,
        runtime_stats: Arc<Op::Stats>,
        input_id: InputId,
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
        })
    }

    fn spawn_sink(
        &mut self,
        tasks: &mut OrderingAwareJoinSet<SinkTaskResult<Op>>,
        op: &Arc<Op>,
        spawner: &ExecutionTaskSpawner,
        input_id: InputId,
        partition: MicroPartition,
    ) {
        let Some(state) = self.states.pop() else {
            return;
        };
        let op = op.clone();
        let spawner = spawner.clone();
        let runtime_stats = self.runtime_stats.clone();
        tasks.spawn(async move {
            let now = Instant::now();
            let state = op.sink(partition, state, runtime_stats, &spawner).await??;
            Ok((input_id, state, now.elapsed()))
        });
    }

    fn flush_pending(
        &mut self,
        tasks: &mut OrderingAwareJoinSet<SinkTaskResult<Op>>,
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
}

pub struct BlockingSinkNode<Op: BlockingSink> {
    op: Arc<Op>,
    child: Box<dyn PipelineNode>,
    meter: Meter,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
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
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    fn spawn_finalize(
        op: Arc<Op>,
        per_input: PerInputState<Op>,
        input_id: InputId,
        finalize_spawner: ExecutionTaskSpawner,
        output_tx: Sender<PipelineMessage>,
        finalize_tasks: &mut JoinSet<DaftResult<()>>,
    ) {
        finalize_tasks.spawn(async move {
            let output = op.finalize(per_input.states, &finalize_spawner).await??;
            for partition in output {
                per_input.runtime_stats.add_rows_out(partition.len() as u64);
                let _ = output_tx
                    .send(PipelineMessage::Morsel {
                        input_id,
                        partition,
                    })
                    .await;
            }
            let _ = output_tx.send(PipelineMessage::Flush(input_id)).await;
            Ok(())
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
    ) -> DaftResult<()> {
        let node_id = node_info.id;
        let max_concurrency = op.max_concurrency();
        let compute_runtime = get_compute_runtime();
        let task_spawner = ExecutionTaskSpawner::new(
            compute_runtime.clone(),
            memory_manager.clone(),
            info_span!("BlockingSink::Sink"),
        );
        let finalize_spawner = ExecutionTaskSpawner::new(
            compute_runtime,
            memory_manager,
            info_span!("BlockingSink::Finalize"),
        );
        let mut inputs: HashMap<InputId, PerInputState<Op>> = HashMap::new();
        let mut tasks: OrderingAwareJoinSet<SinkTaskResult<Op>> = OrderingAwareJoinSet::new(false);
        let mut finalize_tasks: JoinSet<DaftResult<()>> = JoinSet::new();
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
            // Poll finalize tasks to propagate errors
            while let Some(result) = finalize_tasks.try_join_next() {
                result??;
            }

            let per_input_tasks: Vec<_> = inputs
                .iter()
                .map(|(id, s)| {
                    format!(
                        "{}:{}active/{}pending",
                        id,
                        s.max_concurrency - s.states.len(),
                        s.pending.len()
                    )
                })
                .collect();
            println!(
                "[BlockingSink::{op_name}] total_tasks={} inputs={} [{per_input_tasks:?}]",
                tasks.len(),
                inputs.len(),
            );
            match event {
                PipelineEvent::TaskCompleted((input_id, state, elapsed)) => {
                    let per_input = inputs.get_mut(&input_id).unwrap();
                    per_input
                        .runtime_stats
                        .add_duration_us(elapsed.as_micros() as u64);
                    per_input.states.push(state);
                    per_input.flush_pending(&mut tasks, &op, &task_spawner, input_id)?;

                    if per_input.ready_to_finalize() {
                        Self::spawn_finalize(
                            op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            finalize_spawner.clone(),
                            output_tx.clone(),
                            &mut finalize_tasks,
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
                                input_id,
                            )?)
                        }
                    };
                    per_input.runtime_stats.add_rows_in(partition.len() as u64);
                    per_input.pending.push_back(partition);
                    per_input.flush_pending(&mut tasks, &op, &task_spawner, input_id)?;
                }
                PipelineEvent::Flush(input_id) => {
                    if let Some(p) = inputs.get_mut(&input_id) {
                        p.flushed = true;
                    }
                    if inputs.get(&input_id).is_some_and(|p| p.ready_to_finalize()) {
                        Self::spawn_finalize(
                            op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            finalize_spawner.clone(),
                            output_tx.clone(),
                            &mut finalize_tasks,
                        );
                    }
                }
                PipelineEvent::InputClosed => {
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
                            &mut finalize_tasks,
                        );
                    }
                }
            }
        }

        // Drain remaining finalize tasks
        while let Some(result) = finalize_tasks.join_next().await {
            result??;
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
            ..
        } = *self;
        let name: Arc<str> = node_info.name.clone();
        let child_rx = child.start(false, runtime_handle)?;
        let (output_tx, output_rx) = create_channel(1);
        let memory_manager = runtime_handle.memory_manager();
        let stats_manager = runtime_handle.stats_manager();

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
