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
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    runtime_stats::{
        CountingSender, DefaultRuntimeStats, InitializingCountingReceiver, RuntimeStats,
    },
};
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum IntermediateOperatorResult {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    HasMoreOutput {
        input: Arc<MicroPartition>,
        output: Arc<MicroPartition>,
    },
}

pub(crate) type IntermediateOpExecuteResult<Op> = OperatorOutput<
    DaftResult<(
        <Op as IntermediateOperator>::State,
        IntermediateOperatorResult,
    )>,
>;
pub(crate) trait IntermediateOperator: Send + Sync {
    type State: Send + Sync + Unpin;
    type BatchingStrategy: BatchingStrategy + 'static;
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self>;
    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> Self::State;
    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(DefaultRuntimeStats::new(id))
    }
    /// The maximum number of concurrent workers that can be spawned for this operator.
    /// Each worker will has its own IntermediateOperatorState.
    /// This method should be overridden if the operator needs to limit the number of concurrent workers, i.e. UDFs with resource requests.
    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(get_compute_pool_num_threads())
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }

    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy>;
}

pub struct IntermediateNode<Op: IntermediateOperator> {
    intermediate_op: Arc<Op>,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

type StateId = usize;

struct ExecutionTaskResult<S> {
    state_id: StateId,
    state: S,
    result: IntermediateOperatorResult,
    elapsed: Duration,
}

// ========== IntermediateNode Implementation ==========

impl<Op: IntermediateOperator + 'static> IntermediateNode<Op> {
    pub(crate) fn new(
        intermediate_op: Arc<Op>,
        children: Vec<Box<dyn PipelineNode>>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
        output_schema: SchemaRef,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = intermediate_op.name().into();
        let info = ctx.next_node_info(
            name,
            intermediate_op.op_type(),
            NodeCategory::Intermediate,
            output_schema,
            context,
        );
        let runtime_stats = intermediate_op.make_runtime_stats(info.id);
        let morsel_size_requirement = intermediate_op
            .morsel_size_requirement()
            .unwrap_or_default();
        Self {
            intermediate_op,
            children,
            runtime_stats,
            plan_stats,
            morsel_size_requirement,
            node_info: Arc::new(info),
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    // ========== Helper Functions ==========

    fn spawn_execution_task(
        task_set: &mut OrderingAwareJoinSet<DaftResult<ExecutionTaskResult<Op::State>>>,
        input: Arc<MicroPartition>,
        state: Op::State,
        state_id: StateId,
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
    ) {
        task_set.spawn(async move {
            let now = Instant::now();
            let (new_state, result) = op.execute(input, state, &task_spawner).await??;
            let elapsed = now.elapsed();

            Ok(ExecutionTaskResult {
                state_id,
                state: new_state,
                result,
                elapsed,
            })
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_task_completion(
        result: ExecutionTaskResult<Op::State>,
        state_pool: &mut HashMap<StateId, Op::State>,
        output_sender: &CountingSender,
        batch_manager: &Arc<BatchManager<Op::BatchingStrategy>>,
        task_set: &mut OrderingAwareJoinSet<DaftResult<ExecutionTaskResult<Op::State>>>,
        op: Arc<Op>,
        task_spawner: &ExecutionTaskSpawner,
        runtime_stats: Arc<dyn RuntimeStats>,
    ) -> DaftResult<bool> {
        match result.result {
            IntermediateOperatorResult::NeedMoreInput(Some(mp)) => {
                // Record execution stats
                batch_manager.record_execution_stats(runtime_stats, mp.len(), result.elapsed);

                // Send output
                if output_sender.send(mp).await.is_err() {
                    return Ok(false);
                }

                // Return state to pool
                state_pool.insert(result.state_id, result.state);
            }
            IntermediateOperatorResult::NeedMoreInput(None) => {
                // No output, just return state to pool
                state_pool.insert(result.state_id, result.state);
            }
            IntermediateOperatorResult::HasMoreOutput { input, output } => {
                // Record execution stats
                batch_manager.record_execution_stats(runtime_stats, output.len(), result.elapsed);

                // Send output
                if output_sender.send(output).await.is_err() {
                    return Ok(false);
                }

                // Spawn another execution with same input and state (don't return state)
                Self::spawn_execution_task(
                    task_set,
                    input,
                    result.state,
                    result.state_id,
                    op,
                    task_spawner.clone(),
                );
            }
        }
        Ok(true)
    }

    fn spawn_ready_batches(
        buffer: &mut RowBasedBuffer,
        state_pool: &mut HashMap<StateId, Op::State>,
        task_set: &mut OrderingAwareJoinSet<DaftResult<ExecutionTaskResult<Op::State>>>,
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
    ) -> DaftResult<()> {
        // Check buffer for ready batches and spawn tasks while states available
        while !state_pool.is_empty()
            && let Some(batch) = buffer.next_batch_if_ready()?
        {
            let state_id = *state_pool
                .keys()
                .next()
                .expect("State pool should have states when has_available() returns true");
            let state = state_pool
                .remove(&state_id)
                .expect("State pool should have states when has_available() returns true");

            Self::spawn_execution_task(
                task_set,
                batch,
                state,
                state_id,
                op.clone(),
                task_spawner.clone(),
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_input(
        mut receiver: InitializingCountingReceiver,
        output_sender: &CountingSender,
        op: Arc<Op>,
        state_pool: &mut HashMap<StateId, Op::State>,
        batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
        task_spawner: ExecutionTaskSpawner,
        runtime_stats: Arc<dyn RuntimeStats>,
        maintain_order: bool,
    ) -> DaftResult<bool> {
        let mut task_set: OrderingAwareJoinSet<DaftResult<ExecutionTaskResult<Op::State>>> =
            OrderingAwareJoinSet::new(maintain_order);
        let (lower, upper) = batch_manager.initial_requirements().values();
        let mut buffer = RowBasedBuffer::new(lower, upper);
        let mut input_closed = false;

        // Main processing loop
        while !input_closed || !task_set.is_empty() {
            tokio::select! {
                biased;

                // Branch 1: Join completed task (only if tasks exist)
                Some(join_result) = task_set.join_next(), if !task_set.is_empty() => {
                    if !Self::handle_task_completion(
                        join_result??,
                        state_pool,
                        output_sender,
                        &batch_manager,
                        &mut task_set,
                        op.clone(),
                        &task_spawner,
                        runtime_stats.clone(),
                    )
                    .await?
                    {
                        return Ok(false);
                    }

                    // After completing a task, update bounds and try to spawn more tasks
                    let new_requirements = batch_manager.calculate_batch_size();
                    let (lower, upper) = new_requirements.values();
                    buffer.update_bounds(lower, upper);

                    Self::spawn_ready_batches(
                        &mut buffer,
                        state_pool,
                        &mut task_set,
                        op.clone(),
                        task_spawner.clone(),
                    )?;
                }

                // Branch 2: Receive input (only if states available and receiver open)
                morsel = receiver.recv(), if !state_pool.is_empty() && !input_closed => {
                    match morsel {
                        Some(morsel) => {
                            buffer.push(morsel);
                            Self::spawn_ready_batches(
                                &mut buffer,
                                state_pool,
                                &mut task_set,
                                op.clone(),
                                task_spawner.clone(),
                            )?;
                        }
                        None => {
                            input_closed = true;
                        }
                    }
                }
            }
        }

        // After loop exits, verify invariants
        debug_assert_eq!(task_set.len(), 0, "TaskSet should be empty after loop");
        debug_assert!(input_closed, "Receiver should be closed after loop");

        // Handle remaining buffered data
        if let Some(last_batch) = buffer.pop_all()? {
            // Since task_set is empty, all states should be back in the pool
            let state_id = *state_pool
                .keys()
                .next()
                .expect("State pool should have states after all tasks completed");
            let state = state_pool
                .remove(&state_id)
                .expect("State pool should have states after all tasks completed");

            Self::spawn_execution_task(
                &mut task_set,
                last_batch,
                state,
                state_id,
                op.clone(),
                task_spawner.clone(),
            );

            // Wait for final task to complete
            while let Some(join_result) = task_set.join_next().await {
                let result = join_result??;
                if !Self::handle_task_completion(
                    result,
                    state_pool,
                    output_sender,
                    &batch_manager,
                    &mut task_set,
                    op.clone(),
                    &task_spawner,
                    runtime_stats.clone(),
                )
                .await?
                {
                    return Ok(false);
                }
            }
        }

        debug_assert_eq!(task_set.len(), 0, "TaskSet should be empty after loop");

        Ok(true)
    }
}

impl<Op: IntermediateOperator + 'static> TreeDisplay for IntermediateNode<Op> {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        use common_display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.intermediate_op.name()).unwrap();
            }
            level => {
                let multiline_display = self.intermediate_op.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
                writeln!(display, "Batch Size = {}", self.morsel_size_requirement).unwrap();
                if matches!(level, DisplayLevel::Verbose) {
                    writeln!(display).unwrap();
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
            "category": "Intermediate",
            "type": self.intermediate_op.op_type().to_string(),
            "name": self.name(),
            "children": children,
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children.iter().map(|v| v.as_tree_display()).collect()
    }
}

impl<Op: IntermediateOperator + 'static> PipelineNode for IntermediateNode<Op> {
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
        default_requirement: MorselSizeRequirement,
    ) {
        let operator_morsel_size_requirement = self.intermediate_op.morsel_size_requirement();
        let combined_morsel_size_requirement = MorselSizeRequirement::combine_requirements(
            operator_morsel_size_requirement,
            downstream_requirement,
        );
        self.morsel_size_requirement = combined_morsel_size_requirement;
        for child in &mut self.children {
            child.propagate_morsel_size_requirement(
                combined_morsel_size_requirement,
                default_requirement,
            );
        }
    }

    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<tokio::sync::mpsc::Receiver<Arc<MicroPartition>>> {
        // 1. Start children and wrap receivers
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let child_result_receiver = child.start(maintain_order, runtime_handle)?;
            child_result_receivers.push(InitializingCountingReceiver::new(
                child_result_receiver,
                self.node_id(),
                self.runtime_stats.clone(),
                runtime_handle.stats_manager(),
            ));
        }

        // 2. Setup
        let op = self.intermediate_op.clone();
        let max_concurrency = op.max_concurrency().context(PipelineExecutionSnafu {
            node_name: self.name().to_string(),
        })?;
        let (destination_sender, destination_receiver) = tokio::sync::mpsc::channel(1);
        let counting_sender = CountingSender::new(destination_sender, self.runtime_stats.clone());
        let strategy = op.batching_strategy().context(PipelineExecutionSnafu {
            node_name: self.name().to_string(),
        })?;
        let batch_manager = Arc::new(BatchManager::new(strategy));

        // 3. Create task spawner
        let compute_runtime = get_compute_runtime();
        let task_spawner = ExecutionTaskSpawner::new(
            compute_runtime,
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("IntermediateOp::execute"),
        );

        // 4. Spawn process_input task
        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();
        let runtime_stats = self.runtime_stats.clone();
        runtime_handle.spawn(
            async move {
                // Initialize state pool with max_concurrency states
                let mut state_pool = HashMap::new();
                for i in 0..max_concurrency {
                    state_pool.insert(i, op.make_state());
                }

                // Process each child receiver sequentially
                for receiver in child_result_receivers {
                    if !Self::process_input(
                        receiver,
                        &counting_sender,
                        op.clone(),
                        &mut state_pool,
                        batch_manager.clone(),
                        task_spawner.clone(),
                        runtime_stats.clone(),
                        maintain_order,
                    )
                    .await?
                    {
                        break;
                    }
                }

                // Finalize node after processing completes
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
