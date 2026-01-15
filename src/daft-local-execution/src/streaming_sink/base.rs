use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeInfo, NodeType};
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads, get_compute_runtime};
use daft_core::prelude::SchemaRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput, PipelineExecutionSnafu,
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender, create_channel},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
};

#[derive(Clone)]
pub enum StreamingSinkOutput {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput {
        input: Arc<MicroPartition>,
        output: Option<Arc<MicroPartition>>,
    },
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
}

pub struct StreamingSinkNode<Op: StreamingSink> {
    op: Arc<Op>,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
    morsel_size_requirement: MorselSizeRequirement,
}

type StateId = usize;

struct ExecutionTaskResult<S> {
    state_id: StateId,
    state: S,
    output: StreamingSinkOutput,
    elapsed: Duration,
}

struct ExecutionContext<Op: StreamingSink> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    task_set: OrderingAwareJoinSet<DaftResult<ExecutionTaskResult<Op::State>>>,
    state_pool: HashMap<StateId, Op::State>,
    output_sender: Sender<Arc<MicroPartition>>,
    batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    stats_manager: RuntimeStatsManagerHandle,
}

impl<Op: StreamingSink + 'static> StreamingSinkNode<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        children: Vec<Box<dyn PipelineNode>>,
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
            children,
            runtime_stats,
            plan_stats,
            node_info: Arc::new(node_info),
            morsel_size_requirement,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    // ========== Helper Functions ==========

    fn spawn_execution_task(
        ctx: &mut ExecutionContext<Op>,
        input: Arc<MicroPartition>,
        state: Op::State,
        state_id: StateId,
    ) {
        let op = ctx.op.clone();
        let task_spawner = ctx.task_spawner.clone();
        ctx.task_set.spawn(async move {
            let now = Instant::now();
            let (new_state, result) = op.execute(input, state, &task_spawner).await??;
            let elapsed = now.elapsed();

            Ok(ExecutionTaskResult {
                state_id,
                state: new_state,
                output: result,
                elapsed,
            })
        });
    }

    fn spawn_ready_batches(
        buffer: &mut RowBasedBuffer,
        ctx: &mut ExecutionContext<Op>,
    ) -> DaftResult<()> {
        // Check buffer for ready batches and spawn tasks while states available
        while !ctx.state_pool.is_empty()
            && let Some(batch) = buffer.next_batch_if_ready()?
        {
            let state_id = *ctx
                .state_pool
                .keys()
                .next()
                .expect("State pool should have states when it is not empty");
            let state = ctx
                .state_pool
                .remove(&state_id)
                .expect("State pool should have states when it is not empty");

            Self::spawn_execution_task(ctx, batch, state, state_id);
        }
        Ok(())
    }

    async fn handle_task_completion(
        result: ExecutionTaskResult<Op::State>,
        ctx: &mut ExecutionContext<Op>,
    ) -> DaftResult<bool> {
        match result.output {
            StreamingSinkOutput::NeedMoreInput(mp) => {
                // Record execution stats
                ctx.batch_manager.record_execution_stats(
                    ctx.runtime_stats.as_ref(),
                    mp.as_ref().map(|mp| mp.len()).unwrap_or(0),
                    result.elapsed,
                );

                // Send output if present
                if let Some(mp) = mp {
                    ctx.runtime_stats.add_rows_out(mp.len() as u64);
                    if ctx.output_sender.send(mp).await.is_err() {
                        return Ok(false);
                    }
                }

                // Return state to pool
                ctx.state_pool.insert(result.state_id, result.state);
                Ok(true)
            }
            StreamingSinkOutput::HasMoreOutput { input, output } => {
                // Record execution stats
                ctx.batch_manager.record_execution_stats(
                    ctx.runtime_stats.as_ref(),
                    output.as_ref().map(|mp| mp.len()).unwrap_or(0),
                    result.elapsed,
                );

                // Send output
                if let Some(mp) = output {
                    ctx.runtime_stats.add_rows_out(mp.len() as u64);
                    if ctx.output_sender.send(mp).await.is_err() {
                        return Ok(false);
                    }
                }

                // Spawn another execution with same input and state (don't return state)
                Self::spawn_execution_task(ctx, input, result.state, result.state_id);
                Ok(true)
            }
            StreamingSinkOutput::Finished(mp) => {
                // Record execution stats
                ctx.batch_manager.record_execution_stats(
                    ctx.runtime_stats.as_ref(),
                    mp.as_ref().map(|mp| mp.len()).unwrap_or(0),
                    result.elapsed,
                );

                // Send output if present
                if let Some(mp) = mp {
                    ctx.runtime_stats.add_rows_out(mp.len() as u64);
                    let _ = ctx.output_sender.send(mp).await;
                }

                // Return state to pool for finalization
                ctx.state_pool.insert(result.state_id, result.state);
                // Short-circuit: Finished means we should exit early (like a closed sender)
                Ok(false)
            }
        }
    }

    async fn process_input(
        node_id: usize,
        mut receiver: Receiver<Arc<MicroPartition>>,
        ctx: &mut ExecutionContext<Op>,
    ) -> DaftResult<bool> {
        let (lower, upper) = ctx.batch_manager.initial_requirements().values();
        let mut buffer = RowBasedBuffer::new(lower, upper);
        let mut input_closed = false;
        let mut node_initialized = false;

        // Main processing loop
        while !input_closed || !ctx.task_set.is_empty() {
            tokio::select! {
                biased;

                // Branch 1: Join completed task (only if tasks exist)
                Some(join_result) = ctx.task_set.join_next(), if !ctx.task_set.is_empty() => {
                    let result = join_result??;
                    if !Self::handle_task_completion(result, ctx).await? {
                        return Ok(false);
                    }

                    // After completing a task, update bounds and try to spawn more tasks
                    let new_requirements = ctx.batch_manager.calculate_batch_size();
                    let (lower, upper) = new_requirements.values();
                    buffer.update_bounds(lower, upper);

                    Self::spawn_ready_batches(&mut buffer, ctx)?;
                }

                // Branch 2: Receive input (only if states available and receiver open)
                morsel = receiver.recv(), if !ctx.state_pool.is_empty() && !input_closed => {
                    match morsel {
                        Some(morsel) => {
                            if !node_initialized {
                                ctx.stats_manager.activate_node(node_id);
                                node_initialized = true;
                            }
                            ctx.runtime_stats.add_rows_in(morsel.len() as u64);
                            buffer.push(morsel);
                            Self::spawn_ready_batches(&mut buffer, ctx)?;
                        }
                        None => {
                            input_closed = true;
                        }
                    }
                }
            }
        }

        // After loop exits, verify invariants
        debug_assert_eq!(ctx.task_set.len(), 0, "TaskSet should be empty after loop");
        debug_assert!(input_closed, "Receiver should be closed after loop");

        // Handle remaining buffered data
        if let Some(last_batch) = buffer.pop_all()? {
            // Since task_set is empty, all states should be back in the pool
            let state_id = *ctx
                .state_pool
                .keys()
                .next()
                .expect("State pool should have states after all tasks completed");
            let state = ctx
                .state_pool
                .remove(&state_id)
                .expect("State pool should have states after all tasks completed");

            Self::spawn_execution_task(ctx, last_batch, state, state_id);

            // Wait for final task to complete
            while let Some(join_result) = ctx.task_set.join_next().await {
                if !Self::handle_task_completion(join_result??, ctx).await? {
                    return Ok(false);
                }
            }
        }

        debug_assert_eq!(ctx.task_set.len(), 0, "TaskSet should be empty after loop");

        Ok(true)
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
        self.children()
            .iter()
            .map(|v| v.as_tree_display())
            .collect()
    }
}

impl<Op: StreamingSink + 'static> PipelineNode for StreamingSinkNode<Op> {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect()
    }

    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        self.children.iter().collect()
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
        for child in &mut self.children {
            child.propagate_morsel_size_requirement(
                combined_morsel_size_requirement,
                _default_morsel_size,
            );
        }
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        for child in &mut self.children {
            let child_result_receiver = child.start(maintain_order, runtime_handle)?;
            child_result_receivers.push(child_result_receiver);
        }

        let (destination_sender, destination_receiver) = create_channel(1);

        // Initialize state pool with max_concurrency states
        let mut state_pool = HashMap::new();
        for i in 0..self.op.max_concurrency() {
            state_pool.insert(
                i,
                self.op.make_state().context(PipelineExecutionSnafu {
                    node_name: self.op.name().to_string(),
                })?,
            );
        }

        // Create task spawners
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

        let mut ctx = ExecutionContext {
            op: self.op.clone(),
            task_spawner,
            task_set: OrderingAwareJoinSet::new(maintain_order),
            state_pool,
            output_sender: destination_sender,
            batch_manager: Arc::new(BatchManager::new(self.op.batching_strategy())),
            runtime_stats: self.runtime_stats.clone(),
            stats_manager: runtime_handle.stats_manager(),
        };
        let node_id = self.node_id();
        runtime_handle.spawn(
            async move {
                for receiver in child_result_receivers {
                    if !Self::process_input(node_id, receiver, &mut ctx).await? {
                        break;
                    }
                }

                let mut finished_states: Vec<_> =
                    ctx.state_pool.drain().map(|(_, state)| state).collect();
                loop {
                    let finalized_result = ctx
                        .op
                        .finalize(finished_states, &finalize_spawner)
                        .await??;
                    match finalized_result {
                        StreamingSinkFinalizeOutput::HasMoreOutput { states, output } => {
                            if let Some(mp) = output {
                                ctx.runtime_stats.add_rows_out(mp.len() as u64);
                                let _ = ctx.output_sender.send(mp).await;
                            }
                            finished_states = states;
                        }
                        StreamingSinkFinalizeOutput::Finished(output) => {
                            if let Some(mp) = output {
                                ctx.runtime_stats.add_rows_out(mp.len() as u64);
                                let _ = ctx.output_sender.send(mp).await;
                            }
                            break;
                        }
                    }
                }

                ctx.stats_manager.finalize_node(node_id);
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
