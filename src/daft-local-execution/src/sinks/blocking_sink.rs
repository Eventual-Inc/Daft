use std::{num::NonZeroUsize, sync::Arc, time::Instant};

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
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    pipeline_execution::{InputStatesTracker, PipelineEvent, StateTracker, next_event},
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
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
        input_id: InputId,
        state: S,
        elapsed: std::time::Duration,
    },
    Finalize {
        input_id: InputId,
        output: Vec<Arc<MicroPartition>>,
    },
}

struct BlockingSinkProcessor<Op: BlockingSink> {
    task_set: OrderingAwareJoinSet<DaftResult<BlockingSinkTaskResult<Op::State>>>,
    max_concurrency: usize,
    input_state_tracker: InputStatesTracker<Op::State>,
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    finalize_spawner: ExecutionTaskSpawner,
    runtime_stats: Arc<dyn RuntimeStats>,
    output_sender: Sender<PipelineMessage>,
    stats_manager: RuntimeStatsManagerHandle,
    node_id: usize,
    node_initialized: bool,
}

impl<Op: BlockingSink + 'static> BlockingSinkProcessor<Op> {
    fn spawn_sink_task(
        &mut self,
        partition: Arc<MicroPartition>,
        state: Op::State,
        input_id: InputId,
    ) {
        let op = self.op.clone();
        let task_spawner = self.task_spawner.clone();
        self.task_set.spawn(async move {
            let now = Instant::now();
            let new_state = op.sink(partition, state, &task_spawner).await??;
            let elapsed = now.elapsed();
            Ok(BlockingSinkTaskResult::Sink {
                input_id,
                state: new_state,
                elapsed,
            })
        });
    }

    fn try_progress_input(&mut self, input_id: InputId) -> DaftResult<()> {
        // Try to spawn tasks for the input
        while self.task_set.len() < self.max_concurrency
            && let Some(next) = self
                .input_state_tracker
                .get_next_morsel_for_execute(input_id)
        {
            let (partition, state) = next?;
            self.spawn_sink_task(partition, state, input_id);
        }
        // Try to finalize the input
        if self.task_set.len() < self.max_concurrency
            && let Some(states) = self
                .input_state_tracker
                .try_take_states_for_finalize(input_id)
        {
            let op = self.op.clone();
            let finalize_spawner = self.finalize_spawner.clone();
            self.task_set.spawn(async move {
                let output = op.finalize(states, &finalize_spawner).await??;
                Ok(BlockingSinkTaskResult::Finalize { input_id, output })
            });
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

    async fn handle_sink_completed(
        &mut self,
        input_id: InputId,
        state: Op::State,
        elapsed: std::time::Duration,
    ) -> DaftResult<ControlFlow> {
        self.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);
        self.input_state_tracker.return_state(input_id, state);
        self.try_progress_input(input_id)?;
        self.try_progress_all_inputs()?;
        Ok(ControlFlow::Continue)
    }

    async fn handle_finalize_completed(
        &mut self,
        input_id: InputId,
        output: Vec<Arc<MicroPartition>>,
    ) -> DaftResult<ControlFlow> {
        for partition in output {
            self.runtime_stats.add_rows_out(partition.len() as u64);
            if self
                .output_sender
                .send(PipelineMessage::Morsel {
                    input_id,
                    partition,
                })
                .await
                .is_err()
            {
                return Ok(ControlFlow::Stop);
            }
        }
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
        if !self.input_state_tracker.contains_key(input_id)
            && self
                .output_sender
                .send(PipelineMessage::Flush(input_id))
                .await
                .is_err()
        {
            return Ok(ControlFlow::Stop);
        } else {
            self.input_state_tracker.mark_completed(input_id);
            self.try_progress_input(input_id)?;
        }
        Ok(ControlFlow::Continue)
    }

    async fn handle_input_closed(&mut self) -> DaftResult<ControlFlow> {
        self.input_state_tracker.mark_all_completed();
        self.try_progress_all_inputs()?;
        Ok(ControlFlow::Continue)
    }

    async fn start_processing(
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
                PipelineEvent::TaskCompleted(BlockingSinkTaskResult::Sink {
                    input_id,
                    state,
                    elapsed,
                }) => self.handle_sink_completed(input_id, state, elapsed).await?,
                PipelineEvent::TaskCompleted(BlockingSinkTaskResult::Finalize {
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
        let input_state_tracker = InputStatesTracker::new(Box::new(
            move |_input_id| -> DaftResult<StateTracker<Op::State>> {
                let states = (0..op.max_concurrency())
                    .map(|_| op.make_state())
                    .collect::<DaftResult<Vec<_>>>()?;
                Ok(StateTracker::new(
                    states,
                    RowBasedBuffer::new(0, NonZeroUsize::new(usize::MAX).unwrap()),
                ))
            },
        ));

        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let node_id = self.node_id();
        let stats_manager = runtime_handle.stats_manager();
        runtime_handle.spawn(
            async move {
                let mut processor = BlockingSinkProcessor {
                    task_set: OrderingAwareJoinSet::new(maintain_order),
                    max_concurrency: get_compute_pool_num_threads(),
                    input_state_tracker,
                    op,
                    task_spawner,
                    finalize_spawner,
                    runtime_stats,
                    output_sender: destination_sender,
                    stats_manager: stats_manager.clone(),
                    node_id,
                    node_initialized: false,
                };

                processor
                    .start_processing(&mut child_results_receiver)
                    .await?;

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
