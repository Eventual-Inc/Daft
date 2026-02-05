use std::{sync::Arc, time::Instant};

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::{
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::StatSnapshotImpl,
};
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads, get_compute_runtime};
use daft_core::prelude::SchemaRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    channel::{Receiver, create_channel},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    pipeline_execution::{
        InputStateTracker, NodeExecutionHandler, OperatorExecutionOutput, OperatorFinalizeOutput,
        PipelineNodeExecutor, StateTracker, UnifiedFinalizeOutput, UnifiedTaskResult,
    },
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats},
};

pub(crate) type StreamingSinkExecuteResult<Op> =
    OperatorOutput<DaftResult<(<Op as StreamingSink>::State, OperatorExecutionOutput)>>;
pub(crate) type StreamingSinkFinalizeResult<Op> =
    OperatorOutput<DaftResult<OperatorFinalizeOutput<<Op as StreamingSink>::State>>>;
pub(crate) trait StreamingSink: Send + Sync {
    type State: Send + Sync + Unpin;
    type BatchingStrategy: BatchingStrategy + 'static;

    /// Execute the StreamingSink operator on the morsel of input data,
    /// received from the child,
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

    /// The maximum number of concurrent workers per input_id that can be spawned for this sink.
    /// Each worker will has its own StreamingSinkState.
    fn max_concurrency_per_input_id(&self) -> usize {
        get_compute_pool_num_threads()
    }

    /// The maximum total number of concurrent workers across all input_ids.
    fn total_max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }
    fn batching_strategy(&self) -> Self::BatchingStrategy;
}

pub struct StreamingSinkNode<Op: StreamingSink> {
    op: Arc<Op>,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
    morsel_size_requirement: MorselSizeRequirement,
}

struct StreamingSinkHandler<Op: StreamingSink> {
    op: Arc<Op>,
}

impl<Op: StreamingSink + 'static> NodeExecutionHandler<Op::State> for StreamingSinkHandler<Op> {
    fn spawn_task(
        &self,
        task_set: &mut OrderingAwareJoinSet<DaftResult<UnifiedTaskResult<Op::State>>>,
        task_spawner: ExecutionTaskSpawner,
        partition: Arc<MicroPartition>,
        state: Op::State,
        input_id: InputId,
    ) {
        let op = self.op.clone();
        task_set.spawn(async move {
            let now = Instant::now();
            let (new_state, result) = op.execute(partition, state, &task_spawner).await??;
            let elapsed = now.elapsed();

            Ok(UnifiedTaskResult::Execution {
                input_id,
                state: new_state,
                elapsed,
                output: result,
            })
        });
    }

    fn spawn_finalize_task(
        &self,
        task_set: &mut OrderingAwareJoinSet<DaftResult<UnifiedTaskResult<Op::State>>>,
        finalize_spawner: Option<ExecutionTaskSpawner>,
        input_id: InputId,
        states: Vec<Op::State>,
    ) {
        let op = self.op.clone();
        let finalize_spawner = finalize_spawner.expect("finalize_spawner must be set");
        task_set.spawn(async move {
            let finalized_result = op.finalize(states, &finalize_spawner).await??;

            // Convert OperatorFinalizeOutput to UnifiedFinalizeOutput
            let output = match finalized_result {
                OperatorFinalizeOutput::HasMoreOutput { states, output } => {
                    UnifiedFinalizeOutput::Continue { states, output }
                }
                OperatorFinalizeOutput::Finished(output) => {
                    UnifiedFinalizeOutput::Done(output.into_iter().collect())
                }
            };

            Ok(UnifiedTaskResult::Finalize { input_id, output })
        });
    }
}

impl<Op: StreamingSink + 'static> StreamingSinkNode<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        child: Box<dyn PipelineNode>,
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
            child,
            runtime_stats,
            plan_stats,
            node_info: Arc::new(node_info),
            morsel_size_requirement,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
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
            "category": "StreamingSink",
            "type": self.op.op_type().to_string(),
            "name": self.name(),
            "children": children,
        })
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
        _default_morsel_size: MorselSizeRequirement,
    ) {
        let operator_morsel_size_requirement = self.op.morsel_size_requirement();
        let combined_morsel_size_requirement = MorselSizeRequirement::combine_requirements(
            operator_morsel_size_requirement,
            downstream_requirement,
        );
        self.morsel_size_requirement = combined_morsel_size_requirement;
        self.child.propagate_morsel_size_requirement(
            combined_morsel_size_requirement,
            _default_morsel_size,
        );
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let child_result_receiver = self.child.start(maintain_order, runtime_handle)?;
        let (destination_sender, destination_receiver) = create_channel(1);

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

        let op = self.op.clone();
        let input_state_tracker = InputStateTracker::new(Box::new(
            move |_input_id| -> DaftResult<StateTracker<Op::State>> {
                let states = (0..op.max_concurrency_per_input_id())
                    .map(|_| op.make_state())
                    .collect::<DaftResult<Vec<_>>>()?;
                let (lower, upper) = op.batching_strategy().initial_requirements().values();
                Ok(StateTracker::new(states, lower, upper))
            },
        ));

        let mut base = PipelineNodeExecutor::new(
            task_spawner,
            self.runtime_stats.clone(),
            self.op.total_max_concurrency(),
            OrderingAwareJoinSet::new(maintain_order),
            input_state_tracker,
            runtime_handle.stats_manager().clone(),
        )
        .with_finalize_spawner(finalize_spawner)
        .with_output_sender(destination_sender)
        .with_batch_manager(Arc::new(BatchManager::new(self.op.batching_strategy())));

        // Create handler
        let mut handler = StreamingSinkHandler {
            op: self.op.clone(),
        };

        let node_id = self.node_id();
        let stats_manager = runtime_handle.stats_manager().clone();
        runtime_handle.spawn(
            async move {
                base.process_input(child_result_receiver, node_id, &mut handler)
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
