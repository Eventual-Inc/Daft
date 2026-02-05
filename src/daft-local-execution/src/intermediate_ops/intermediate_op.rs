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
use snafu::ResultExt;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput, PipelineExecutionSnafu,
    channel::{Receiver, create_channel},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    pipeline_execution::{
        InputStateTracker, NodeExecutionHandler, OperatorExecutionOutput, PipelineNodeExecutor,
        StateTracker, UnifiedTaskResult,
    },
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats},
};
pub(crate) type IntermediateOpExecuteResult<Op> =
    OperatorOutput<DaftResult<(<Op as IntermediateOperator>::State, OperatorExecutionOutput)>>;
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
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

type OperatorStateId = usize;

struct IntermediateNodeHandler<Op: IntermediateOperator> {
    op: Arc<Op>,
}

impl<Op: IntermediateOperator + 'static> NodeExecutionHandler<(OperatorStateId, Op::State)>
    for IntermediateNodeHandler<Op>
{
    fn spawn_task(
        &self,
        task_set: &mut OrderingAwareJoinSet<
            DaftResult<UnifiedTaskResult<(OperatorStateId, Op::State)>>,
        >,
        task_spawner: ExecutionTaskSpawner,
        partition: Arc<MicroPartition>,
        state: (OperatorStateId, Op::State),
        input_id: InputId,
    ) {
        let (state_id, op_state) = state;
        let op = self.op.clone();
        task_set.spawn(async move {
            let now = Instant::now();
            let (new_op_state, result) = op.execute(partition, op_state, &task_spawner).await??;
            let elapsed = now.elapsed();

            // Return UnifiedTaskResult::Execution
            Ok(UnifiedTaskResult::Execution {
                input_id,
                state: (state_id, new_op_state),
                elapsed,
                output: result,
            })
        });
    }
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
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        // 1. Start children and wrap receivers
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        for child in &mut self.children {
            let child_result_receiver = child.start(maintain_order, runtime_handle)?;
            child_result_receivers.push(child_result_receiver);
        }
        let (destination_sender, destination_receiver) = create_channel(1);

        // 3. Create task spawner
        let task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("IntermediateOp::execute"),
        );

        // Create batch manager and task set
        let batch_manager = Arc::new(BatchManager::new(
            self.intermediate_op
                .batching_strategy()
                .context(PipelineExecutionSnafu {
                    node_name: self.intermediate_op.name().to_string(),
                })?,
        ));
        let task_set = OrderingAwareJoinSet::new(maintain_order);

        // Create input state tracker with cloned operator states per input_id
        let initial_requirements = batch_manager.initial_requirements();
        let (lower, upper) = initial_requirements.values();
        let op = self.intermediate_op.clone();
        let input_state_tracker = InputStateTracker::new(Box::new(
            move |_input_id| -> DaftResult<StateTracker<(OperatorStateId, Op::State)>> {
                // Create max_concurrency cloned operator states per input_id
                let mut states = Vec::with_capacity(op.max_concurrency());
                for i in 0..op.max_concurrency() {
                    states.push((i, op.make_state()));
                }
                Ok(StateTracker::new(states, lower, upper))
            },
        ));

        // Create base execution context
        let mut executor = PipelineNodeExecutor::new(
            task_spawner,
            self.runtime_stats.clone(),
            self.intermediate_op.max_concurrency(),
            task_set,
            input_state_tracker,
            runtime_handle.stats_manager().clone(),
        )
        .with_output_sender(destination_sender)
        .with_batch_manager(batch_manager);

        // Create handler
        let mut handler = IntermediateNodeHandler {
            op: self.intermediate_op.clone(),
        };
        let node_id = self.node_id();
        let stats_manager = runtime_handle.stats_manager().clone();
        runtime_handle.spawn(
            async move {
                for receiver in child_result_receivers {
                    if executor
                        .process_input(receiver, node_id, &mut handler)
                        .await?
                        == crate::ControlFlow::Stop
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
