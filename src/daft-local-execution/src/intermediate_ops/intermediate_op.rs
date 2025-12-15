use std::{sync::Arc, time::Instant};

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeInfo, NodeType};
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_core::prelude::SchemaRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::{info_span, instrument};

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput, PipelineExecutionSnafu,
    channel::{
        OrderingAwareReceiver, Receiver, Sender, create_channel,
        create_ordering_aware_receiver_channel,
    },
    dispatcher::{DispatchSpawner, RoundRobinDispatcher, UnorderedDispatcher},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    resource_manager::MemoryManager,
    runtime_stats::{
        CountingSender, DefaultRuntimeStats, InitializingCountingReceiver, RuntimeStats,
    },
};
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum IntermediateOperatorResult {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    HasMoreOutput(Arc<MicroPartition>),
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
    fn make_state(&self) -> DaftResult<Self::State>;
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

    fn dispatch_spawner(
        &self,
        batch_manager: Arc<BatchManager<Self::BatchingStrategy>>,
        maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        if maintain_order {
            Arc::new(RoundRobinDispatcher::new(batch_manager))
        } else {
            Arc::new(UnorderedDispatcher::new(
                batch_manager.initial_requirements(),
            ))
        }
    }
}

pub struct IntermediateNode<Op: IntermediateOperator> {
    intermediate_op: Arc<Op>,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

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

    #[instrument(level = "info", skip_all, name = "IntermediateOperator::run_worker")]
    pub async fn run_worker(
        op: Arc<Op>,
        receiver: Receiver<Arc<MicroPartition>>,
        sender: Sender<Arc<MicroPartition>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        memory_manager: Arc<MemoryManager>,
        batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    ) -> DaftResult<()> {
        let span = info_span!("IntermediateOp::execute");
        let compute_runtime = get_compute_runtime();
        let task_spawner =
            ExecutionTaskSpawner::new(compute_runtime, memory_manager, runtime_stats.clone(), span);
        let mut state = op.make_state()?;
        while let Some(morsel) = receiver.recv().await {
            loop {
                let now = Instant::now();
                let result = op.execute(morsel.clone(), state, &task_spawner).await??;
                let elapsed = now.elapsed();

                state = result.0;
                match result.1 {
                    IntermediateOperatorResult::NeedMoreInput(Some(mp)) => {
                        batch_manager.record_execution_stats(
                            runtime_stats.clone(),
                            mp.len(),
                            elapsed,
                        );

                        if sender.send(mp).await.is_err() {
                            return Ok(());
                        }
                        break;
                    }
                    IntermediateOperatorResult::NeedMoreInput(None) => {
                        break;
                    }
                    IntermediateOperatorResult::HasMoreOutput(mp) => {
                        batch_manager.record_execution_stats(
                            runtime_stats.clone(),
                            mp.len(),
                            elapsed,
                        );
                        if sender.send(mp).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn spawn_workers(
        &self,
        input_receivers: Vec<Receiver<Arc<MicroPartition>>>,
        runtime_handle: &mut ExecutionRuntimeContext,
        maintain_order: bool,
        memory_manager: Arc<MemoryManager>,
        batch_manager: Arc<BatchManager<Op::BatchingStrategy>>,
    ) -> OrderingAwareReceiver<Arc<MicroPartition>> {
        let (output_sender, output_receiver) =
            create_ordering_aware_receiver_channel(maintain_order, input_receivers.len());
        for (input_receiver, output_sender) in input_receivers.into_iter().zip(output_sender) {
            runtime_handle.spawn(
                Self::run_worker(
                    self.intermediate_op.clone(),
                    input_receiver,
                    output_sender,
                    self.runtime_stats.clone(),
                    memory_manager.clone(),
                    batch_manager.clone(),
                ),
                &self.intermediate_op.name(),
            );
        }
        output_receiver
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
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
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
        let op = self.intermediate_op.clone();
        let num_workers = op.max_concurrency().context(PipelineExecutionSnafu {
            node_name: self.name().to_string(),
        })?;

        let (destination_sender, destination_receiver) = create_channel(0);
        let counting_sender = CountingSender::new(destination_sender, self.runtime_stats.clone());
        let strategy = op.batching_strategy().context(PipelineExecutionSnafu {
            node_name: self.name().to_string(),
        })?;
        let batch_manager = Arc::new(BatchManager::new(strategy));
        let dispatch_spawner = self
            .intermediate_op
            .dispatch_spawner(batch_manager.clone(), maintain_order);
        let spawned_dispatch_result = dispatch_spawner.spawn_dispatch(
            child_result_receivers,
            num_workers,
            &mut runtime_handle.handle(),
        );
        runtime_handle.spawn(
            async move { spawned_dispatch_result.spawned_dispatch_task.await? },
            &self.name(),
        );

        let mut output_receiver = self.spawn_workers(
            spawned_dispatch_result.worker_receivers,
            runtime_handle,
            maintain_order,
            runtime_handle.memory_manager(),
            batch_manager,
        );
        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();
        runtime_handle.spawn(
            async move {
                while let Some(morsel) = output_receiver.recv().await {
                    if counting_sender.send(morsel).await.is_err() {
                        return Ok(());
                    }
                }
                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &op.name(),
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
