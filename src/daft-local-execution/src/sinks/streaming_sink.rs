use std::sync::Arc;

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
    dispatcher::DispatchSpawner,
    pipeline::PipelineNode,
    progress_bar::ProgressBarColor,
    resource_manager::MemoryManager,
    runtime_stats::{CountingReceiver, CountingSender, RuntimeStatsContext},
    ExecutionRuntimeContext, ExecutionTaskSpawner, JoinSnafu, OperatorOutput, TaskSet,
};

pub trait StreamingSinkState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub enum StreamingSinkOutput {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput(Arc<MicroPartition>),
    Finished(Option<Arc<MicroPartition>>),
}

pub(crate) type StreamingSinkExecuteResult =
    OperatorOutput<DaftResult<(Box<dyn StreamingSinkState>, StreamingSinkOutput)>>;
pub(crate) type StreamingSinkFinalizeResult =
    OperatorOutput<DaftResult<Option<Arc<MicroPartition>>>>;
pub trait StreamingSink: Send + Sync {
    /// Execute the StreamingSink operator on the morsel of input data,
    /// received from the child with the given index,
    /// with the given state.
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn StreamingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult;

    /// Finalize the StreamingSink operator, with the given states from each worker.
    fn finalize(
        &self,
        states: Vec<Box<dyn StreamingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult;

    /// The name of the StreamingSink operator.
    fn name(&self) -> &'static str;

    fn multiline_display(&self) -> Vec<String>;

    /// Create a new worker-local state for this StreamingSink.
    fn make_state(&self) -> Box<dyn StreamingSinkState>;

    /// The maximum number of concurrent workers that can be spawned for this sink.
    /// Each worker will has its own StreamingSinkState.
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn dispatch_spawner(
        &self,
        runtime_handle: &ExecutionRuntimeContext,
        maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner>;
}

pub struct StreamingSinkNode {
    op: Arc<dyn StreamingSink>,
    name: &'static str,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<RuntimeStatsContext>,
    plan_stats: StatsState,
}

impl StreamingSinkNode {
    pub(crate) fn new(
        op: Arc<dyn StreamingSink>,
        children: Vec<Box<dyn PipelineNode>>,
        plan_stats: StatsState,
    ) -> Self {
        let name = op.name();
        Self {
            op,
            name,
            children,
            runtime_stats: RuntimeStatsContext::new(name),
            plan_stats,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    #[instrument(level = "info", skip_all, name = "StreamingSink::run_worker")]
    async fn run_worker(
        op: Arc<dyn StreamingSink>,
        input_receiver: Receiver<Arc<MicroPartition>>,
        output_sender: Sender<Arc<MicroPartition>>,
        rt_context: Arc<RuntimeStatsContext>,
        memory_manager: Arc<MemoryManager>,
    ) -> DaftResult<Box<dyn StreamingSinkState>> {
        let span = info_span!("StreamingSink::Execute");
        let compute_runtime = get_compute_runtime();
        let spawner = ExecutionTaskSpawner::new(compute_runtime, memory_manager, rt_context, span);
        let mut state = op.make_state();
        while let Some(morsel) = input_receiver.recv().await {
            loop {
                let result = op.execute(morsel.clone(), state, &spawner).await??;
                state = result.0;
                match result.1 {
                    StreamingSinkOutput::NeedMoreInput(mp) => {
                        if let Some(mp) = mp {
                            if output_sender.send(mp).await.is_err() {
                                return Ok(state);
                            }
                        }
                        break;
                    }
                    StreamingSinkOutput::HasMoreOutput(mp) => {
                        if output_sender.send(mp).await.is_err() {
                            return Ok(state);
                        }
                    }
                    StreamingSinkOutput::Finished(mp) => {
                        if let Some(mp) = mp {
                            let _ = output_sender.send(mp).await;
                        }
                        return Ok(state);
                    }
                }
            }
        }

        Ok(state)
    }

    fn spawn_workers(
        op: Arc<dyn StreamingSink>,
        input_receivers: Vec<Receiver<Arc<MicroPartition>>>,
        task_set: &mut TaskSet<DaftResult<Box<dyn StreamingSinkState>>>,
        stats: Arc<RuntimeStatsContext>,
        maintain_order: bool,
        memory_manager: Arc<MemoryManager>,
    ) -> OrderingAwareReceiver<Arc<MicroPartition>> {
        let (output_sender, output_receiver) =
            create_ordering_aware_receiver_channel(maintain_order, input_receivers.len());
        for (input_receiver, output_sender) in input_receivers.into_iter().zip(output_sender) {
            task_set.spawn(Self::run_worker(
                op.clone(),
                input_receiver,
                output_sender,
                stats.clone(),
                memory_manager.clone(),
            ));
        }
        output_receiver
    }
}

impl TreeDisplay for StreamingSinkNode {
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
                    let rt_result = self.runtime_stats.result();
                    rt_result.display(&mut display, true, true, true).unwrap();
                }
            }
        }
        display
    }
    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children()
            .iter()
            .map(|v| v.as_tree_display())
            .collect()
    }
}

impl PipelineNode for StreamingSinkNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect()
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let progress_bar = runtime_handle.make_progress_bar(
            self.name(),
            ProgressBarColor::Cyan,
            true,
            self.runtime_stats.clone(),
        );
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let child_result_receiver = child.start(maintain_order, runtime_handle)?;
            child_result_receivers.push(CountingReceiver::new(
                child_result_receiver,
                self.runtime_stats.clone(),
                progress_bar.clone(),
            ));
        }

        let (destination_sender, destination_receiver) = create_channel(0);
        let counting_sender =
            CountingSender::new(destination_sender, self.runtime_stats.clone(), progress_bar);

        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let num_workers = op.max_concurrency();

        let dispatch_spawner = op.dispatch_spawner(runtime_handle, maintain_order);
        let spawned_dispatch_result = dispatch_spawner.spawn_dispatch(
            child_result_receivers,
            num_workers,
            &mut runtime_handle.handle(),
        );
        runtime_handle.spawn(
            async move { spawned_dispatch_result.spawned_dispatch_task.await? },
            self.name(),
        );

        let memory_manager = runtime_handle.memory_manager();
        runtime_handle.spawn(
            async move {
                let mut task_set = TaskSet::new();
                let mut output_receiver = Self::spawn_workers(
                    op.clone(),
                    spawned_dispatch_result.worker_receivers,
                    &mut task_set,
                    runtime_stats.clone(),
                    maintain_order,
                    memory_manager.clone(),
                );

                while let Some(morsel) = output_receiver.recv().await {
                    if counting_sender.send(morsel).await.is_err() {
                        break;
                    }
                }

                let mut finished_states = Vec::with_capacity(num_workers);
                while let Some(result) = task_set.join_next().await {
                    let state = result.context(JoinSnafu)??;
                    finished_states.push(state);
                }

                let compute_runtime = get_compute_runtime();
                let spawner = ExecutionTaskSpawner::new(
                    compute_runtime,
                    memory_manager,
                    runtime_stats.clone(),
                    info_span!("StreamingSink::Finalize"),
                );
                let finalized_result = op.finalize(finished_states, &spawner).await??;
                if let Some(res) = finalized_result {
                    let _ = counting_sender.send(res).await;
                }
                Ok(())
            },
            self.name(),
        );
        Ok(destination_receiver)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
