use std::{
    collections::HashMap,
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
    pipeline_execution::{PipelineEvent, next_event},
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
};

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(Clone)]
pub(crate) enum IntermediateOperatorResult {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput {
        input: Arc<MicroPartition>,
        output: Option<Arc<MicroPartition>>,
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
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }

    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy>;
}

pub struct IntermediateNode<Op: IntermediateOperator> {
    intermediate_op: Arc<Op>,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

struct ExecutionTaskResult<S> {
    input_id: InputId,
    state: S,
    result: IntermediateOperatorResult,
    elapsed: Duration,
}

struct InputState {
    buffer: RowBasedBuffer,
    in_flight: usize,
    pending_flush: bool,
}

impl InputState {
    fn can_flush(&self) -> bool {
        self.pending_flush && self.in_flight == 0 && self.buffer.is_empty()
    }

    fn next_batch_if_ready(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        let batch = self.buffer.next_batch_if_ready()?;
        if batch.is_some() {
            Ok(batch)
        } else if self.pending_flush {
            self.buffer.pop_all()
        } else {
            Ok(None)
        }
    }
}

struct IntermediateOpProcessor<Op: IntermediateOperator> {
    task_set: OrderingAwareJoinSet<DaftResult<ExecutionTaskResult<Op::State>>>,
    available_states: Vec<Op::State>,
    input_states: HashMap<InputId, InputState>,
    batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    runtime_stats: Arc<dyn RuntimeStats>,
    output_sender: Sender<PipelineMessage>,
    stats_manager: RuntimeStatsManagerHandle,
    node_id: usize,
    node_initialized: bool,
    op_name: Arc<str>,
    input_start_times: HashMap<InputId, std::time::Instant>,
}

/// input_id lifecycle (no finalize step):
///
/// 1. BUFFER  — morsel arrives, data buffered
/// 2. EXECUTE — tasks spawned while batch_size met AND under max_concurrency
/// 3. FLUSH   — flush received → pending_flush=true → pop_all for remaining buffer
/// 4. CLEANUP — buffer empty + no in-flight → remove input_id → propagate flush downstream
impl<Op: IntermediateOperator + 'static> IntermediateOpProcessor<Op> {
    fn new(
        maintain_order: bool,
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
        runtime_stats: Arc<dyn RuntimeStats>,
        output_sender: Sender<PipelineMessage>,
        stats_manager: RuntimeStatsManagerHandle,
        node_id: usize,
        op_name: Arc<str>,
    ) -> DaftResult<Self> {
        let batch_manager = Arc::new(BatchManager::new(op.batching_strategy()?));
        let available_states = (0..op.max_concurrency()).map(|_| op.make_state()).collect();
        Ok(Self {
            task_set: OrderingAwareJoinSet::new(maintain_order),
            available_states,
            input_states: HashMap::new(),
            batch_manager,
            op,
            task_spawner,
            runtime_stats,
            output_sender,
            stats_manager,
            node_id,
            node_initialized: false,
            op_name,
            input_start_times: HashMap::new(),
        })
    }

    #[inline]
    async fn send(&self, msg: PipelineMessage) -> ControlFlow {
        if self.output_sender.send(msg).await.is_err() {
            ControlFlow::Stop
        } else {
            ControlFlow::Continue
        }
    }

    /// Spawn a single task for the given input_id, consuming a state from the pool.
    fn spawn_task(
        input_id: InputId,
        op_state: Op::State,
        batch: Arc<MicroPartition>,
        task_set: &mut OrderingAwareJoinSet<DaftResult<ExecutionTaskResult<Op::State>>>,
        op: &Arc<Op>,
        task_spawner: &ExecutionTaskSpawner,
    ) {
        let op = op.clone();
        let task_spawner = task_spawner.clone();
        task_set.spawn(async move {
            let now = Instant::now();
            let (new_op_state, result) = op.execute(batch, op_state, &task_spawner).await??;
            let elapsed = now.elapsed();
            Ok(ExecutionTaskResult {
                input_id,
                state: new_op_state,
                result,
                elapsed,
            })
        });
    }

    fn try_spawn_tasks(&mut self) -> DaftResult<()> {
        let mut input_ids: Vec<InputId> = self.input_states.keys().copied().collect();
        input_ids.sort_unstable();
        for input_id in input_ids {
            if self.task_set.len() >= self.op.max_concurrency() || self.available_states.is_empty()
            {
                break;
            }
            self.try_spawn_tasks_for_input(input_id)?;
        }
        Ok(())
    }

    fn try_spawn_tasks_for_input(&mut self, input_id: InputId) -> DaftResult<()> {
        let input_state = self
            .input_states
            .get_mut(&input_id)
            .expect("Input should be present");
        while !self.available_states.is_empty()
            && let Some(input) = input_state.next_batch_if_ready()?
        {
            let op_state = self.available_states.pop().unwrap();
            input_state.in_flight += 1;
            Self::spawn_task(
                input_id,
                op_state,
                input,
                &mut self.task_set,
                &self.op,
                &self.task_spawner,
            );
        }
        Ok(())
    }

    async fn try_flush_input(&mut self, input_id: InputId) -> DaftResult<ControlFlow> {
        let input_state = self
            .input_states
            .get_mut(&input_id)
            .expect("Input should be present");
        if input_state.can_flush() {
            self.input_states.remove(&input_id);
            if let Some(start) = self.input_start_times.remove(&input_id) {
                println!(
                    "[Daft] [{:.3}] {} input_id={} finished in {:.3}s",
                    crate::epoch_secs(), self.op_name, input_id, start.elapsed().as_secs_f64()
                );
            }
            if self.send(PipelineMessage::Flush(input_id)).await == ControlFlow::Stop {
                return Ok(ControlFlow::Stop);
            }
            return Ok(ControlFlow::Continue);
        }
        Ok(ControlFlow::Continue)
    }

    async fn handle_task_completed(
        &mut self,
        task_result: ExecutionTaskResult<Op::State>,
    ) -> DaftResult<ControlFlow> {
        let ExecutionTaskResult {
            input_id,
            state,
            result,
            elapsed,
        } = task_result;

        self.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

        if let Some(mp) = result.output() {
            self.runtime_stats.add_rows_out(mp.len() as u64);
            self.batch_manager.record_execution_stats(
                self.runtime_stats.as_ref(),
                mp.len(),
                elapsed,
            );
            if self
                .send(PipelineMessage::Morsel {
                    input_id,
                    partition: mp.clone(),
                })
                .await
                == ControlFlow::Stop
            {
                return Ok(ControlFlow::Stop);
            }
        }

        let new_requirements = self.batch_manager.calculate_batch_size();
        for input in self.input_states.values_mut() {
            input.buffer.update_bounds(new_requirements);
        }

        match result {
            IntermediateOperatorResult::NeedMoreInput(_) => {
                self.input_states.get_mut(&input_id).unwrap().in_flight -= 1;
                self.available_states.push(state);
                self.try_spawn_tasks()?;
                self.try_flush_input(input_id).await
            }
            IntermediateOperatorResult::HasMoreOutput { input, .. } => {
                Self::spawn_task(
                    input_id,
                    state,
                    input,
                    &mut self.task_set,
                    &self.op,
                    &self.task_spawner,
                );
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
        self.input_start_times
            .entry(input_id)
            .or_insert_with(std::time::Instant::now);
        self.runtime_stats.add_rows_in(partition.len() as u64);
        let (lower, upper) = self.batch_manager.calculate_batch_size().values();
        let input = self
            .input_states
            .entry(input_id)
            .or_insert_with(|| InputState {
                buffer: RowBasedBuffer::new(lower, upper),
                in_flight: 0,
                pending_flush: false,
            });
        input.buffer.push(partition);
        self.try_spawn_tasks()
    }

    async fn handle_flush(&mut self, input_id: InputId) -> DaftResult<ControlFlow> {
        let Some(input) = self.input_states.get_mut(&input_id) else {
            // Never seen this input_id, forward flush immediately.
            return Ok(self.send(PipelineMessage::Flush(input_id)).await);
        };
        input.pending_flush = true;
        self.try_spawn_tasks()?;
        self.try_flush_input(input_id).await
    }

    async fn handle_input_closed(&mut self) -> DaftResult<ControlFlow> {
        for input_state in self.input_states.values_mut() {
            input_state.pending_flush = true;
        }
        self.try_spawn_tasks()?;
        let mut input_ids: Vec<InputId> = self.input_states.keys().copied().collect();
        input_ids.sort_unstable();
        for input_id in input_ids {
            if self.try_flush_input(input_id).await? == ControlFlow::Stop {
                return Ok(ControlFlow::Stop);
            }
        }
        Ok(ControlFlow::Continue)
    }

    async fn process_input(&mut self, receiver: &mut Receiver<PipelineMessage>) -> DaftResult<()> {
        let mut input_closed = false;

        while !input_closed || !self.task_set.is_empty() || !self.input_states.is_empty() {
            let event = next_event(
                &mut self.task_set,
                self.op.max_concurrency(),
                receiver,
                &mut input_closed,
            )
            .await?;
            let cf = match event {
                PipelineEvent::TaskCompleted(task_result) => {
                    self.handle_task_completed(task_result).await?
                }
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
                return Ok(());
            }
        }
        Ok(())
    }
}

// ========== IntermediateNode Implementation ==========

impl<Op: IntermediateOperator + 'static> IntermediateNode<Op> {
    pub(crate) fn new(
        intermediate_op: Arc<Op>,
        child: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = intermediate_op.name().into();
        let info = ctx.next_node_info(
            name,
            intermediate_op.op_type(),
            NodeCategory::Intermediate,
            context,
        );
        let runtime_stats = intermediate_op.make_runtime_stats(info.id);
        let morsel_size_requirement = intermediate_op
            .morsel_size_requirement()
            .unwrap_or_default();
        Self {
            intermediate_op,
            child,
            runtime_stats,
            plan_stats,
            morsel_size_requirement,
            node_info: Arc::new(info),
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

impl IntermediateOperatorResult {
    /// Get the output partition if present.
    pub(crate) fn output(&self) -> Option<&Arc<MicroPartition>> {
        match self {
            Self::NeedMoreInput(mp) => mp.as_ref(),
            Self::HasMoreOutput { output, .. } => output.as_ref(),
        }
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
            "type": self.intermediate_op.op_type().to_string(),
            "name": self.name(),
            "children": children,
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

impl<Op: IntermediateOperator + 'static> PipelineNode for IntermediateNode<Op> {
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
        default_requirement: MorselSizeRequirement,
    ) {
        let operator_morsel_size_requirement = self.intermediate_op.morsel_size_requirement();
        let combined_morsel_size_requirement = MorselSizeRequirement::combine_requirements(
            operator_morsel_size_requirement,
            downstream_requirement,
        );
        self.morsel_size_requirement = combined_morsel_size_requirement;
        self.child.propagate_morsel_size_requirement(
            combined_morsel_size_requirement,
            default_requirement,
        );
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        // 1. Start children and wrap receivers
        let mut child_result_receiver = self.child.start(maintain_order, runtime_handle)?;
        let (destination_sender, destination_receiver) = create_channel(1);

        // 2. Create task spawner
        let task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("IntermediateOp::execute"),
        );

        let op = self.intermediate_op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let node_id = self.node_id();
        let op_name = self.name();
        let stats_manager = runtime_handle.stats_manager();
        runtime_handle.spawn(
            async move {
                let mut processor = IntermediateOpProcessor::new(
                    maintain_order,
                    op,
                    task_spawner,
                    runtime_stats,
                    destination_sender,
                    stats_manager.clone(),
                    node_id,
                    op_name,
                )?;

                processor.process_input(&mut child_result_receiver).await?;

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
