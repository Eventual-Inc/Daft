use std::{
    collections::HashMap,
    ops::ControlFlow,
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
use snafu::ResultExt;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput, PipelineExecutionSnafu,
    batch_manager::BatchManager,
    channel::{Receiver, Sender, create_channel},
    dynamic_batching::BatchingStrategy,
    pipeline::{
        BuilderContext, InputId, MorselSizeRequirement, NodeName, PipelineEvent, PipelineMessage,
        PipelineNode, next_event,
    },
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
};

pub type IntermediateOperatorResult = MicroPartition;

pub(crate) type IntermediateOpExecuteResult<Op> = OperatorOutput<
    DaftResult<(
        <Op as IntermediateOperator>::State,
        IntermediateOperatorResult,
    )>,
>;
pub(crate) trait IntermediateOperator: Send + Sync {
    type State: Send + Sync + Unpin;
    type Stats: RuntimeStats = DefaultRuntimeStats;
    type BatchingStrategy: BatchingStrategy + 'static;
    fn execute(
        &self,
        input: MicroPartition,
        state: Self::State,
        runtime_stats: Arc<Self::Stats>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self>;
    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> Self::State;
    /// The maximum number of concurrent workers that can be spawned for this operator.
    /// Each worker has its own state.
    /// Override if the operator needs to limit concurrency, e.g. UDFs with resource requests.
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
    meter: Meter,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

struct WorkerResult<Op: IntermediateOperator> {
    state: Op::State,
    input_id: InputId,
    result: IntermediateOperatorResult,
    elapsed: Duration,
}

struct ExecutionContext<Op: IntermediateOperator> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    task_set: OrderingAwareJoinSet<DaftResult<WorkerResult<Op>>>,
    operator_states: Vec<Op::State>,
    output_sender: Sender<PipelineMessage>,
    batch_manager: BatchManager<Op::BatchingStrategy>,
    active_workers: HashMap<InputId, usize>,
    per_input_stats: HashMap<InputId, Arc<Op::Stats>>,
    stats_manager: RuntimeStatsManagerHandle,
    meter: Meter,
    node_info: Arc<NodeInfo>,
    node_id: usize,
    node_initialized: bool,
}

impl<Op: IntermediateOperator + 'static> ExecutionContext<Op> {
    fn get_or_create_stats(&mut self, input_id: InputId) -> Arc<Op::Stats> {
        self.per_input_stats
            .entry(input_id)
            .or_insert_with(|| {
                let stats = Arc::new(Op::Stats::new(&self.meter, &self.node_info));
                self.stats_manager
                    .register_runtime_stats(self.node_id, input_id, stats.clone());
                stats
            })
            .clone()
    }

    fn dispatch_ready_batches(&mut self, input_id: InputId) -> DaftResult<()> {
        while !self.operator_states.is_empty() {
            let Some(batch) = self.batch_manager.next_batch(input_id)? else {
                break;
            };
            *self.active_workers.entry(input_id).or_insert(0) += 1;
            let state = self.operator_states.pop().unwrap();
            let op = self.op.clone();
            let task_spawner = self.task_spawner.clone();
            let runtime_stats = self.get_or_create_stats(input_id);
            self.task_set.spawn(async move {
                let now = Instant::now();
                let (new_state, result) = op
                    .execute(batch, state, runtime_stats, &task_spawner)
                    .await??;
                let elapsed = now.elapsed();
                Ok(WorkerResult {
                    state: new_state,
                    input_id,
                    result,
                    elapsed,
                })
            });
        }
        Ok(())
    }

    fn try_dispatch(&mut self) -> DaftResult<()> {
        let mut input_ids: Vec<InputId> = self.batch_manager.input_ids().collect();
        input_ids.sort_unstable();
        for input_id in input_ids {
            if self.operator_states.is_empty() {
                break;
            }
            self.dispatch_ready_batches(input_id)?;
        }
        Ok(())
    }

    async fn try_flush_input(&mut self, input_id: InputId) -> DaftResult<ControlFlow<(), ()>> {
        let workers_idle = self.active_workers.get(&input_id).copied().unwrap_or(0) == 0;
        if workers_idle && self.batch_manager.can_flush(input_id) {
            _ = self.batch_manager.drain(input_id)?;
            self.active_workers.remove(&input_id);
            self.per_input_stats.remove(&input_id);
            if self
                .output_sender
                .send(PipelineMessage::Flush(input_id))
                .await
                .is_err()
            {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    async fn handle_worker_result(
        &mut self,
        result: WorkerResult<Op>,
    ) -> DaftResult<ControlFlow<(), ()>> {
        let WorkerResult {
            state,
            input_id,
            result,
            elapsed,
        } = result;

        let runtime_stats = self.get_or_create_stats(input_id);
        runtime_stats.add_duration_us(elapsed.as_micros() as u64);
        runtime_stats.add_num_tasks(1);
        self.batch_manager
            .record_completion(runtime_stats.as_ref(), result.len(), elapsed);
        runtime_stats.add_rows_out(result.len() as u64);
        runtime_stats.add_bytes_out(result.size_bytes() as u64);

        if self
            .output_sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition: result,
            })
            .await
            .is_err()
        {
            return Ok(ControlFlow::Break(()));
        }

        *self.active_workers.get_mut(&input_id).unwrap() -= 1;
        self.operator_states.push(state);
        self.try_dispatch()?;
        self.try_flush_input(input_id).await
    }

    async fn process_input(&mut self, receiver: &mut Receiver<PipelineMessage>) -> DaftResult<()> {
        let mut input_closed = false;

        while let Some(event) = next_event(
            &mut self.task_set,
            self.op.max_concurrency(),
            receiver,
            &mut input_closed,
        )
        .await?
        {
            let cf = match event {
                PipelineEvent::TaskCompleted(task_result) => {
                    self.handle_worker_result(task_result).await?
                }
                PipelineEvent::Morsel {
                    input_id,
                    partition,
                } => {
                    if !self.node_initialized {
                        self.stats_manager.activate_node(self.node_id);
                        self.node_initialized = true;
                    }
                    let stats = self.get_or_create_stats(input_id);
                    stats.add_rows_in(partition.len() as u64);
                    stats.add_bytes_in(partition.size_bytes() as u64);
                    self.batch_manager.push(input_id, partition);
                    self.try_dispatch()?;
                    ControlFlow::Continue(())
                }
                PipelineEvent::Flush(input_id) => {
                    if !self.batch_manager.has_input(input_id) {
                        let _ = self
                            .output_sender
                            .send(PipelineMessage::Flush(input_id))
                            .await;
                        return Ok(());
                    }
                    self.batch_manager.set_pending_flush(input_id);
                    self.try_dispatch()?;
                    self.try_flush_input(input_id).await?
                }
                PipelineEvent::FlightPartitionRef => {
                    unreachable!(
                        "IntermediateNode should not receive flight partition refs from child"
                    )
                }
                PipelineEvent::InputClosed => {
                    self.batch_manager.set_all_pending_flush();
                    self.try_dispatch()?;
                    let mut input_ids: Vec<InputId> = self.batch_manager.input_ids().collect();
                    input_ids.sort_unstable();
                    let mut cf = ControlFlow::Continue(());
                    for input_id in input_ids {
                        if self.try_flush_input(input_id).await?.is_break() {
                            cf = ControlFlow::Break(());
                            break;
                        }
                    }
                    cf
                }
            };
            if cf.is_break() {
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
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = intermediate_op.name().into();
        let info = ctx.next_node_info(
            name,
            intermediate_op.op_type(),
            NodeCategory::Intermediate,
            context,
        );
        let morsel_size_requirement = intermediate_op
            .morsel_size_requirement()
            .unwrap_or_default();
        Self {
            intermediate_op,
            child,
            meter: ctx.meter.clone(),
            plan_stats,
            morsel_size_requirement,
            node_info: Arc::new(info),
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
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
            _ => {
                let multiline_display = self.intermediate_op.multiline_display().join("\n");
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
            "category": "Intermediate",
            "type": self.intermediate_op.op_type().to_string(),
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
        self: Box<Self>,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let node_id = self.node_id();
        let name = self.name();

        let mut child_result_receiver = self.child.start(maintain_order, runtime_handle)?;
        let op = self.intermediate_op.clone();
        let (destination_sender, destination_receiver) = create_channel(1);

        let compute_runtime = get_compute_runtime();
        let task_spawner = ExecutionTaskSpawner::new(
            compute_runtime,
            runtime_handle.memory_manager(),
            info_span!("IntermediateOp::execute"),
        );

        let stats_manager = runtime_handle.stats_manager();
        let meter = self.meter.clone();
        let node_info = self.node_info.clone();
        runtime_handle.spawn(
            async move {
                let operator_states: Vec<Op::State> =
                    (0..op.max_concurrency()).map(|_| op.make_state()).collect();

                let batch_manager =
                    BatchManager::new(op.batching_strategy().context(PipelineExecutionSnafu {
                        node_name: op.name().to_string(),
                    })?);

                let mut ctx = ExecutionContext {
                    op,
                    task_spawner,
                    task_set: OrderingAwareJoinSet::new(maintain_order),
                    operator_states,
                    output_sender: destination_sender,
                    batch_manager,
                    active_workers: HashMap::new(),
                    per_input_stats: HashMap::new(),
                    stats_manager: stats_manager.clone(),
                    meter,
                    node_info,
                    node_id,
                    node_initialized: false,
                };
                ctx.process_input(&mut child_result_receiver).await?;

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &name,
        );

        Ok(destination_receiver)
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
