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
use opentelemetry::metrics::Meter;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    channel::{Receiver, Sender, create_channel},
    pipeline::{BuilderContext, MorselSizeRequirement, NodeName, PipelineNode},
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
    fn make_runtime_stats(&self, meter: &Meter, name: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(DefaultRuntimeStats::new(meter, name))
    }
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
}

/// Process all morsels for a single input_id, finalize, and send output.
async fn process_single_input<Op: BlockingSink + 'static>(
    input_id: InputId,
    mut receiver: Receiver<PipelineMessage>,
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    finalize_spawner: ExecutionTaskSpawner,
    runtime_stats: Arc<dyn RuntimeStats>,
    output_sender: Sender<PipelineMessage>,
) -> DaftResult<ControlFlow<()>> {
    let mut state_pool: Vec<Op::State> = (0..op.max_concurrency())
        .map(|_| op.make_state())
        .collect::<DaftResult<_>>()?;
    let mut task_set: JoinSet<DaftResult<(Op::State, Duration)>> = JoinSet::new();
    let mut input_closed = false;

    while !input_closed || !task_set.is_empty() {
        tokio::select! {
            biased;

            // Branch 1: Join completed task (only if tasks exist)
            Some(result) = task_set.join_next(), if !task_set.is_empty() => {
                let (state, elapsed) = result??;
                runtime_stats.add_cpu_us(elapsed.as_micros() as u64);
                state_pool.push(state);
            }

            // Branch 2: Receive input (only if states available and receiver open)
            msg = receiver.recv(), if !state_pool.is_empty() && !input_closed => {
                match msg {
                    Some(PipelineMessage::Morsel { partition, .. }) => {
                        runtime_stats.add_rows_in(partition.len() as u64);
                        let state = state_pool.pop().unwrap();
                        let op = op.clone();
                        let task_spawner = task_spawner.clone();
                        task_set.spawn(async move {
                            let now = Instant::now();
                            let new_state = op.sink(partition, state, &task_spawner).await??;
                            Ok((new_state, now.elapsed()))
                        });
                    }
                    Some(PipelineMessage::Flush(_)) | None => {
                        input_closed = true;
                    }
                }
            }
        }
    }

    // Finalize
    let finalized_outputs = op.finalize(state_pool, &finalize_spawner).await??;
    for partition in finalized_outputs {
        runtime_stats.add_rows_out(partition.len() as u64);
        if output_sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition,
            })
            .await
            .is_err()
        {
            return Ok(ControlFlow::Break(()));
        }
    }
    if output_sender
        .send(PipelineMessage::Flush(input_id))
        .await
        .is_err()
    {
        return Ok(ControlFlow::Break(()));
    }
    Ok(ControlFlow::Continue(()))
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
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = op.name().into();
        let node_info = ctx.next_node_info(name, op.op_type(), NodeCategory::BlockingSink, context);
        let runtime_stats = op.make_runtime_stats(&ctx.meter, node_info.id);

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
        _maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let mut child_results_receiver = self.child.start(false, runtime_handle)?;

        let (destination_sender, destination_receiver) = create_channel(1);

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
                                processor_set.spawn(async move {
                                    process_single_input(
                                        input_id, rx, op, task_spawner,
                                        finalize_spawner, runtime_stats, output_sender,
                                    )
                                    .await
                                });
                            }

                            let is_flush = matches!(&msg, PipelineMessage::Flush(_));
                            let _ = per_input_senders[&input_id].send(msg).await;
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
