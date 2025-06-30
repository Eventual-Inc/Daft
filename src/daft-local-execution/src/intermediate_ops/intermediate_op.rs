use std::sync::Arc;

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::{info_span, instrument};

use crate::{
    channel::{
        create_channel, create_ordering_aware_receiver_channel, OrderingAwareReceiver, Receiver,
        Sender,
    },
    dispatcher::{DispatchSpawner, RoundRobinDispatcher, UnorderedDispatcher},
    pipeline::{NodeInfo, PipelineNode, RuntimeContext},
    progress_bar::ProgressBarColor,
    resource_manager::MemoryManager,
    runtime_stats::{
        BaseStatsBuilder, CountingReceiver, CountingSender, RuntimeStatsBuilder,
        RuntimeStatsContext, RuntimeStatsEventHandler,
    },
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput, PipelineExecutionSnafu,
};

pub(crate) trait IntermediateOpState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

struct DefaultIntermediateOperatorState {}
impl IntermediateOpState for DefaultIntermediateOperatorState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub enum IntermediateOperatorResult {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    HasMoreOutput(Arc<MicroPartition>),
}

pub(crate) type IntermediateOpExecuteResult =
    OperatorOutput<DaftResult<(Box<dyn IntermediateOpState>, IntermediateOperatorResult)>>;
pub trait IntermediateOperator: Send + Sync {
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult;
    fn name(&self) -> &'static str;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(DefaultIntermediateOperatorState {}))
    }
    fn make_runtime_stats_builder(&self) -> Arc<dyn RuntimeStatsBuilder> {
        Arc::new(BaseStatsBuilder {})
    }
    /// The maximum number of concurrent workers that can be spawned for this operator.
    /// Each worker will has its own IntermediateOperatorState.
    /// This method should be overridden if the operator needs to limit the number of concurrent workers, i.e. UDFs with resource requests.
    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(get_compute_pool_num_threads())
    }

    fn morsel_size_range(&self, runtime_handle: &ExecutionRuntimeContext) -> (usize, usize) {
        (0, runtime_handle.default_morsel_size())
    }

    fn dispatch_spawner(
        &self,
        runtime_handle: &ExecutionRuntimeContext,
        maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        let (lower_bound, upper_bound) = self.morsel_size_range(runtime_handle);

        if maintain_order {
            Arc::new(RoundRobinDispatcher::new(lower_bound, upper_bound))
        } else {
            Arc::new(UnorderedDispatcher::new(lower_bound, upper_bound))
        }
    }
}

pub struct IntermediateNode {
    intermediate_op: Arc<dyn IntermediateOperator>,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<RuntimeStatsContext>,
    plan_stats: StatsState,
    node_info: NodeInfo,
}

impl IntermediateNode {
    pub(crate) fn new(
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<Box<dyn PipelineNode>>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
    ) -> Self {
        let info = ctx.next_node_info(intermediate_op.name());

        let rts = RuntimeStatsContext::new_with_builder(
            info.clone(),
            intermediate_op.make_runtime_stats_builder(),
        );
        Self::new_with_runtime_stats(intermediate_op, children, rts, plan_stats, info)
    }

    pub(crate) fn new_with_runtime_stats(
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<Box<dyn PipelineNode>>,
        runtime_stats: Arc<RuntimeStatsContext>,
        plan_stats: StatsState,
        node_info: NodeInfo,
    ) -> Self {
        Self {
            intermediate_op,
            children,
            runtime_stats,
            plan_stats,
            node_info,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    #[instrument(level = "info", skip_all, name = "IntermediateOperator::run_worker")]
    pub async fn run_worker(
        op: Arc<dyn IntermediateOperator>,
        receiver: Receiver<Arc<MicroPartition>>,
        sender: Sender<Arc<MicroPartition>>,
        rt_context: Arc<RuntimeStatsContext>,
        rt_stats_handler: Arc<RuntimeStatsEventHandler>,
        memory_manager: Arc<MemoryManager>,
    ) -> DaftResult<()> {
        let span = info_span!("IntermediateOp::execute");
        let compute_runtime = get_compute_runtime();
        let task_spawner = ExecutionTaskSpawner::new(
            compute_runtime,
            memory_manager,
            rt_context,
            rt_stats_handler,
            span,
        );
        let mut state = op.make_state()?;
        while let Some(morsel) = receiver.recv().await {
            loop {
                let result = op.execute(morsel.clone(), state, &task_spawner).await??;
                state = result.0;
                match result.1 {
                    IntermediateOperatorResult::NeedMoreInput(Some(mp)) => {
                        if sender.send(mp).await.is_err() {
                            return Ok(());
                        }
                        break;
                    }
                    IntermediateOperatorResult::NeedMoreInput(None) => {
                        break;
                    }
                    IntermediateOperatorResult::HasMoreOutput(mp) => {
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
    ) -> OrderingAwareReceiver<Arc<MicroPartition>> {
        let (output_sender, output_receiver) =
            create_ordering_aware_receiver_channel(maintain_order, input_receivers.len());
        for (input_receiver, output_sender) in input_receivers.into_iter().zip(output_sender) {
            runtime_handle.spawn_local(
                Self::run_worker(
                    self.intermediate_op.clone(),
                    input_receiver,
                    output_sender,
                    self.runtime_stats.clone(),
                    runtime_handle.runtime_stats_handler(),
                    memory_manager.clone(),
                ),
                self.intermediate_op.name(),
            );
        }
        output_receiver
    }
}

impl TreeDisplay for IntermediateNode {
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
                if matches!(level, DisplayLevel::Verbose) {
                    writeln!(display).unwrap();
                    let rt_result = self.runtime_stats.render();
                    for (name, value) in rt_result {
                        writeln!(display, "{} = {}", name.capitalize(), value).unwrap();
                    }
                }
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children.iter().map(|v| v.as_tree_display()).collect()
    }
}

impl PipelineNode for IntermediateNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect()
    }

    fn name(&self) -> &'static str {
        self.intermediate_op.name()
    }

    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        let progress_bar = runtime_handle.make_progress_bar(
            self.name(),
            ProgressBarColor::Magenta,
            self.node_id(),
            self.runtime_stats.clone(),
        );

        for child in &self.children {
            let child_result_receiver = child.start(maintain_order, runtime_handle)?;
            child_result_receivers.push(CountingReceiver::new(
                child_result_receiver,
                self.runtime_stats.clone(),
                progress_bar.clone(),
                runtime_handle.runtime_stats_handler(),
            ));
        }
        let op = self.intermediate_op.clone();
        let num_workers = op.max_concurrency().context(PipelineExecutionSnafu {
            node_name: self.name(),
        })?;
        let (destination_sender, destination_receiver) = create_channel(0);
        let counting_sender = CountingSender::new(
            destination_sender,
            self.runtime_stats.clone(),
            progress_bar,
            runtime_handle.runtime_stats_handler(),
        );

        let dispatch_spawner = self
            .intermediate_op
            .dispatch_spawner(runtime_handle, maintain_order);
        let spawned_dispatch_result = dispatch_spawner.spawn_dispatch(
            child_result_receivers,
            num_workers,
            &mut runtime_handle.handle(),
        );
        runtime_handle.spawn_local(
            async move { spawned_dispatch_result.spawned_dispatch_task.await? },
            self.name(),
        );

        let mut output_receiver = self.spawn_workers(
            spawned_dispatch_result.worker_receivers,
            runtime_handle,
            maintain_order,
            runtime_handle.memory_manager(),
        );
        runtime_handle.spawn_local(
            async move {
                while let Some(morsel) = output_receiver.recv().await {
                    if counting_sender.send(morsel).await.is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            },
            op.name(),
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
}
