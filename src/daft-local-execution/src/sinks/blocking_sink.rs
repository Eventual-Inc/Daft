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
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
    channel::{Receiver, create_channel},
    dynamic_batching::StaticBatchingStrategy,
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    pipeline_execution::{
        InputStateTracker, NodeExecutionHandler, PipelineNodeExecutor, StateTracker,
        UnifiedFinalizeOutput, UnifiedTaskResult,
    },
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
    fn make_runtime_stats(&self, name: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(DefaultRuntimeStats::new(name))
    }
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
}

struct BlockingSinkHandler<Op: BlockingSink> {
    op: Arc<Op>,
}

impl<Op: BlockingSink + 'static> NodeExecutionHandler<Op::State> for BlockingSinkHandler<Op>
where
    Op::State: 'static,
{
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
            let new_state = op.sink(partition, state, &task_spawner).await??;
            let elapsed = now.elapsed();

            Ok(UnifiedTaskResult::Execution {
                input_id,
                state: new_state,
                elapsed,
                output: crate::pipeline_execution::OperatorExecutionOutput::NeedMoreInput(None),
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
            let output = op.finalize(states, &finalize_spawner).await??;

            Ok(UnifiedTaskResult::Finalize {
                input_id,
                output: UnifiedFinalizeOutput::Done(output),
            })
        });
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
        let child_results_receiver = self.child.start(false, runtime_handle)?;

        let (destination_sender, destination_receiver) = create_channel(1);

        // Create task spawner
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
        let input_state_tracker = InputStateTracker::new(Box::new(
            move |_input_id| -> DaftResult<StateTracker<Op::State>> {
                let states = (0..op.max_concurrency())
                    .map(|_| op.make_state())
                    .collect::<DaftResult<Vec<_>>>()?;
                // Blocking sink doesn't need batching, so use bounds 0 and usize::MAX
                Ok(StateTracker::new(
                    states,
                    0,
                    NonZeroUsize::new(usize::MAX).unwrap(),
                ))
            },
        ));

        // Create base execution context
        let mut executor: PipelineNodeExecutor<Op::State, StaticBatchingStrategy> =
            PipelineNodeExecutor::new(
                task_spawner,
                self.runtime_stats.clone(),
                get_compute_pool_num_threads(),
                OrderingAwareJoinSet::new(maintain_order),
                input_state_tracker,
                runtime_handle.stats_manager().clone(),
            )
            .with_finalize_spawner(finalize_spawner)
            .with_output_sender(destination_sender);

        // Create handler
        let mut handler = BlockingSinkHandler {
            op: self.op.clone(),
        };
        let node_id = self.node_id();
        let stats_manager = runtime_handle.stats_manager().clone();
        runtime_handle.spawn(
            async move {
                executor
                    .process_input(child_results_receiver, node_id, &mut handler)
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
