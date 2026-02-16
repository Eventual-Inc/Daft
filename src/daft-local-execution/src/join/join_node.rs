use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::{
    ops::{NodeCategory, NodeInfo},
    snapshot::StatSnapshotImpl,
};
use common_runtime::{OrderingAwareJoinSet, get_compute_runtime};
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tokio::sync::oneshot;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorControlFlow,
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender, create_channel},
    dynamic_batching::{BatchManager, StaticBatchingStrategy},
    join::join_operator::{JoinOperator, ProbeOutput},
    pipeline::{BuilderContext, MorselSizeRequirement, PipelineNode},
    runtime_stats::{RuntimeStats, RuntimeStatsManagerHandle},
};

pub struct JoinNode<Op: JoinOperator> {
    op: Arc<Op>,
    left: Box<dyn PipelineNode>,
    right: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

type StateId = usize;

struct BuildTaskResult<Op: JoinOperator> {
    state: Op::BuildState,
    elapsed: Duration,
}

struct ProbeTaskResult<Op: JoinOperator> {
    state_id: StateId,
    state: Op::ProbeState,
    output: ProbeOutput,
    elapsed: Duration,
}

struct BuildExecutionContext<Op: JoinOperator> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    task_set: OrderingAwareJoinSet<DaftResult<BuildTaskResult<Op>>>,
    build_state: Option<Op::BuildState>,
    runtime_stats: Arc<dyn RuntimeStats>,
}

struct ProbeExecutionContext<Op: JoinOperator> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    task_set: OrderingAwareJoinSet<DaftResult<ProbeTaskResult<Op>>>,
    state_pool: HashMap<StateId, Op::ProbeState>,
    output_sender: Sender<Arc<MicroPartition>>,
    batch_manager: Arc<BatchManager<StaticBatchingStrategy>>,
    runtime_stats: Arc<dyn RuntimeStats>,
}

impl<Op: JoinOperator + 'static> JoinNode<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        left: Box<dyn PipelineNode>,
        right: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = op.name().into();
        let node_info = ctx.next_node_info(name, op.op_type(), NodeCategory::Intermediate, context);
        let runtime_stats = op.make_runtime_stats(&ctx.meter, node_info.id);

        let morsel_size_requirement = op.morsel_size_requirement().unwrap_or_default();
        Self {
            op,
            left,
            right,
            runtime_stats,
            plan_stats,
            morsel_size_requirement,
            node_info: Arc::new(node_info),
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    fn spawn_build_task(
        ctx: &mut BuildExecutionContext<Op>,
        input: Arc<MicroPartition>,
        state: Op::BuildState,
    ) {
        let op = ctx.op.clone();
        let task_spawner = ctx.task_spawner.clone();
        ctx.task_set.spawn(async move {
            let now = Instant::now();
            let new_state = op.build(input, state, &task_spawner).await??;
            let elapsed = now.elapsed();

            Ok(BuildTaskResult {
                state: new_state,
                elapsed,
            })
        });
    }

    async fn process_build_input(
        node_id: usize,
        mut receiver: Receiver<Arc<MicroPartition>>,
        ctx: &mut BuildExecutionContext<Op>,
        stats_manager: &RuntimeStatsManagerHandle,
        finalized_build_state_sender: oneshot::Sender<Op::FinalizedBuildState>,
    ) -> DaftResult<()> {
        let mut input_closed = false;
        let mut node_initialized = false;

        // Main processing loop
        while !input_closed || !ctx.task_set.is_empty() {
            tokio::select! {
                biased;

                // Branch 1: Join completed task (only if tasks exist)
                Some(result) = ctx.task_set.join_next(), if !ctx.task_set.is_empty() => {
                    let BuildTaskResult { state, elapsed } = result??;
                    ctx.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

                    // Return state
                    ctx.build_state = Some(state);
                }

                // Branch 2: Receive input (only if state available and receiver open)
                morsel = receiver.recv(), if ctx.build_state.is_some() && !input_closed => {
                    match morsel {
                        Some(morsel) => {
                            if !node_initialized {
                                stats_manager.activate_node(node_id);
                                node_initialized = true;
                            }
                            ctx.runtime_stats.add_rows_in(morsel.len() as u64);
                            let state = ctx.build_state.take()
                                .expect("Build state should be available for build task if ctx.build_state is some");

                            Self::spawn_build_task(ctx, morsel, state);
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

        // Finalize build state
        let build_state = ctx
            .build_state
            .take()
            .expect("Build state should be available for finalize build");
        let finalized = ctx.op.finalize_build(build_state)?;
        // Send finalized build state to probe side
        let _ = finalized_build_state_sender.send(finalized);

        Ok(())
    }

    fn spawn_probe_task(
        ctx: &mut ProbeExecutionContext<Op>,
        input: Arc<MicroPartition>,
        state: Op::ProbeState,
        state_id: StateId,
    ) {
        let op = ctx.op.clone();
        let task_spawner = ctx.task_spawner.clone();
        ctx.task_set.spawn(async move {
            let now = Instant::now();
            let (new_state, result) = op.probe(input, state, &task_spawner).await??;
            let elapsed = now.elapsed();

            Ok(ProbeTaskResult {
                state_id,
                state: new_state,
                output: result,
                elapsed,
            })
        });
    }

    async fn handle_probe_task_completion(
        result: ProbeTaskResult<Op>,
        ctx: &mut ProbeExecutionContext<Op>,
    ) -> DaftResult<OperatorControlFlow> {
        match result.output {
            ProbeOutput::NeedMoreInput(mp) => {
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
                        return Ok(OperatorControlFlow::Break);
                    }
                }

                // Return state to pool
                ctx.state_pool.insert(result.state_id, result.state);
            }
            ProbeOutput::HasMoreOutput { input, output } => {
                // Record execution stats
                ctx.batch_manager.record_execution_stats(
                    ctx.runtime_stats.as_ref(),
                    output.len(),
                    result.elapsed,
                );

                // Send output
                ctx.runtime_stats.add_rows_out(output.len() as u64);
                if ctx.output_sender.send(output).await.is_err() {
                    return Ok(OperatorControlFlow::Break);
                }

                // Spawn another execution with same input and state (don't return state)
                Self::spawn_probe_task(ctx, input, result.state, result.state_id);
            }
        }
        Ok(OperatorControlFlow::Continue)
    }

    fn spawn_ready_probe_batches(
        buffer: &mut RowBasedBuffer,
        ctx: &mut ProbeExecutionContext<Op>,
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

            Self::spawn_probe_task(ctx, batch, state, state_id);
        }
        Ok(())
    }

    async fn process_probe_input(
        node_id: usize,
        mut receiver: Receiver<Arc<MicroPartition>>,
        ctx: &mut ProbeExecutionContext<Op>,
        stats_manager: &RuntimeStatsManagerHandle,
        finalized_build_state_receiver: oneshot::Receiver<Op::FinalizedBuildState>,
    ) -> DaftResult<OperatorControlFlow> {
        // Wait for finalized build state before starting
        let finalized_build_state = match finalized_build_state_receiver.await {
            Ok(build_state) => build_state,
            Err(_) => return Ok(OperatorControlFlow::Break),
        };

        // Initialize probe states with the finalized build state
        let max_probe_concurrency = ctx.op.max_probe_concurrency();
        for i in 0..max_probe_concurrency {
            let probe_state = ctx.op.make_probe_state(finalized_build_state.clone());
            ctx.state_pool.insert(i, probe_state);
        }

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
                    let control = Self::handle_probe_task_completion(join_result??, ctx).await?;
                    if !control.should_continue() {
                        return Ok(OperatorControlFlow::Break);
                    }

                    // After completing a task, update bounds and try to spawn more tasks
                    let new_requirements = ctx.batch_manager.calculate_batch_size();
                    let (lower, upper) = new_requirements.values();
                    buffer.update_bounds(lower, upper);

                    Self::spawn_ready_probe_batches(&mut buffer, ctx)?;
                }

                // Branch 2: Receive input (only if states available and receiver open)
                morsel = receiver.recv(), if !ctx.state_pool.is_empty() && !input_closed => {
                    match morsel {
                        Some(morsel) => {
                            if !node_initialized {
                                stats_manager.activate_node(node_id);
                                node_initialized = true;
                            }
                            ctx.runtime_stats.add_rows_in(morsel.len() as u64);
                            buffer.push(morsel);
                            Self::spawn_ready_probe_batches(&mut buffer, ctx)?;
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

            Self::spawn_probe_task(ctx, last_batch, state, state_id);

            // Wait for final task to complete
            while let Some(join_result) = ctx.task_set.join_next().await {
                let control = Self::handle_probe_task_completion(join_result??, ctx).await?;
                if !control.should_continue() {
                    return Ok(OperatorControlFlow::Break);
                }
            }
        }

        debug_assert_eq!(ctx.task_set.len(), 0, "TaskSet should be empty after loop");

        Ok(OperatorControlFlow::Continue)
    }
}

impl<Op: JoinOperator + 'static> TreeDisplay for JoinNode<Op> {
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
            "category": "Intermediate",
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

impl<Op: JoinOperator + 'static> PipelineNode for JoinNode<Op> {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }

    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        vec![&self.left, &self.right]
    }

    fn name(&self) -> Arc<str> {
        self.node_info.name.clone()
    }

    fn propagate_morsel_size_requirement(
        &mut self,
        downstream_requirement: MorselSizeRequirement,
        default_requirement: MorselSizeRequirement,
    ) {
        let operator_morsel_size_requirement = self.op.morsel_size_requirement();
        // Right side (probe): behave like IntermediateNode - combine operator requirement
        // with downstream requirement
        let right_morsel_size_requirement = MorselSizeRequirement::combine_requirements(
            operator_morsel_size_requirement,
            downstream_requirement,
        );

        // Use the right side requirement for the join node itself
        self.morsel_size_requirement = right_morsel_size_requirement;

        self.left
            .propagate_morsel_size_requirement(default_requirement, default_requirement);
        self.right
            .propagate_morsel_size_requirement(right_morsel_size_requirement, default_requirement);
    }

    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let build_child_receiver = self.left.start(false, runtime_handle)?;
        let probe_child_receiver = self.right.start(maintain_order, runtime_handle)?;

        let (destination_sender, destination_receiver) = create_channel(1);

        // Create task spawners
        let build_task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("JoinNode::Build"),
        );
        let probe_task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("JoinNode::Probe"),
        );
        let finalize_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("JoinNode::FinalizeProbe"),
        );

        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();
        let runtime_stats = self.runtime_stats.clone();
        let op_name = self.op.name().to_string();
        let op = self.op.clone();
        runtime_handle.spawn(
            async move {
                // Create oneshot channel for finalized build state
                let (build_state_sender, build_state_receiver) = oneshot::channel();

                // Phase 1: Initialize build side
                let mut build_ctx = BuildExecutionContext {
                    op: op.clone(),
                    task_spawner: build_task_spawner,
                    task_set: OrderingAwareJoinSet::new(false),
                    build_state: Some(op.make_build_state()?),
                    runtime_stats: runtime_stats.clone(),
                };

                // Phase 2: Initialize probe side (will wait for build state)
                let mut probe_ctx = ProbeExecutionContext {
                    op: op.clone(),
                    task_spawner: probe_task_spawner,
                    task_set: OrderingAwareJoinSet::new(maintain_order),
                    state_pool: HashMap::new(), // Will be initialized in process_probe_input
                    output_sender: destination_sender,
                    batch_manager: Arc::new(BatchManager::new(StaticBatchingStrategy::new(
                        op.morsel_size_requirement().unwrap_or_default(),
                    ))),
                    runtime_stats: runtime_stats.clone(),
                };

                // Spawn both processes concurrently
                let (build_result, probe_result) = tokio::join!(
                    Self::process_build_input(
                        node_id,
                        build_child_receiver,
                        &mut build_ctx,
                        &stats_manager,
                        build_state_sender,
                    ),
                    Self::process_probe_input(
                        node_id,
                        probe_child_receiver,
                        &mut probe_ctx,
                        &stats_manager,
                        build_state_receiver,
                    ),
                );

                build_result?;
                if !probe_result?.should_continue() {
                    stats_manager.finalize_node(node_id);
                    return Ok(());
                }

                // Phase 3: Finalize probe if needed
                if op.needs_probe_finalization() {
                    let finished_states: Vec<_> = probe_ctx
                        .state_pool
                        .drain()
                        .map(|(_, state)| state)
                        .collect();

                    let finalized_output = op
                        .finalize_probe(finished_states, &finalize_spawner)
                        .await??;
                    if let Some(mp) = finalized_output {
                        probe_ctx.runtime_stats.add_rows_out(mp.len() as u64);
                        let _ = probe_ctx.output_sender.send(mp).await;
                    }
                }

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &op_name,
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
