use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::{info_span, instrument};

use crate::{
    channel::{create_channel, Receiver},
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    pipeline::PipelineNode,
    progress_bar::ProgressBarColor,
    resource_manager::MemoryManager,
    runtime_stats::{CountingReceiver, CountingSender, RuntimeStatsContext},
    ExecutionRuntimeContext, ExecutionTaskSpawner, JoinSnafu, OperatorOutput, TaskSet,
};
pub trait BlockingSinkState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub enum BlockingSinkStatus {
    NeedMoreInput(Box<dyn BlockingSinkState>),
    #[allow(dead_code)]
    Finished(Box<dyn BlockingSinkState>),
}

pub(crate) type BlockingSinkSinkResult = OperatorOutput<DaftResult<BlockingSinkStatus>>;
pub(crate) type BlockingSinkFinalizeResult =
    OperatorOutput<DaftResult<Option<Arc<MicroPartition>>>>;
pub trait BlockingSink: Send + Sync {
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult;
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult;
    fn name(&self) -> &'static str;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>>;
    fn dispatch_spawner(
        &self,
        runtime_handle: &ExecutionRuntimeContext,
    ) -> Arc<dyn DispatchSpawner> {
        Arc::new(UnorderedDispatcher::with_fixed_threshold(
            runtime_handle.default_morsel_size(),
        ))
    }
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
}

pub struct BlockingSinkNode {
    op: Arc<dyn BlockingSink>,
    name: &'static str,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<RuntimeStatsContext>,
    plan_stats: StatsState,
}

impl BlockingSinkNode {
    pub(crate) fn new(
        op: Arc<dyn BlockingSink>,
        child: Box<dyn PipelineNode>,
        plan_stats: StatsState,
    ) -> Self {
        let name = op.name();
        Self {
            op,
            name,
            child,
            runtime_stats: RuntimeStatsContext::new(name),
            plan_stats,
        }
    }
    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    #[instrument(level = "info", skip_all, name = "BlockingSink::run_worker")]
    async fn run_worker(
        op: Arc<dyn BlockingSink>,
        input_receiver: Receiver<Arc<MicroPartition>>,
        rt_context: Arc<RuntimeStatsContext>,
        memory_manager: Arc<MemoryManager>,
    ) -> DaftResult<Box<dyn BlockingSinkState>> {
        let span = info_span!("BlockingSink::Sink");
        let compute_runtime = get_compute_runtime();
        let spawner = ExecutionTaskSpawner::new(compute_runtime, memory_manager, rt_context, span);
        let mut state = op.make_state()?;
        while let Some(morsel) = input_receiver.recv().await {
            let result = op.sink(morsel, state, &spawner).await??;
            match result {
                BlockingSinkStatus::NeedMoreInput(new_state) => {
                    state = new_state;
                }
                BlockingSinkStatus::Finished(new_state) => {
                    return Ok(new_state);
                }
            }
        }

        Ok(state)
    }

    fn spawn_workers(
        op: Arc<dyn BlockingSink>,
        input_receivers: Vec<Receiver<Arc<MicroPartition>>>,
        task_set: &mut TaskSet<DaftResult<Box<dyn BlockingSinkState>>>,
        stats: Arc<RuntimeStatsContext>,
        memory_manager: Arc<MemoryManager>,
    ) {
        for input_receiver in input_receivers {
            task_set.spawn(Self::run_worker(
                op.clone(),
                input_receiver,
                stats.clone(),
                memory_manager.clone(),
            ));
        }
    }
}

impl TreeDisplay for BlockingSinkNode {
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
        vec![self.child.as_tree_display()]
    }
}

impl PipelineNode for BlockingSinkNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn start(
        &self,
        _maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let progress_bar = runtime_handle.make_progress_bar(
            self.name(),
            ProgressBarColor::Cyan,
            true,
            self.runtime_stats.clone(),
        );
        let child_results_receiver = self.child.start(false, runtime_handle)?;
        let counting_receiver = CountingReceiver::new(
            child_results_receiver,
            self.runtime_stats.clone(),
            progress_bar.clone(),
        );

        let (destination_sender, destination_receiver) = create_channel(0);
        let counting_sender =
            CountingSender::new(destination_sender, self.runtime_stats.clone(), progress_bar);

        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let num_workers = op.max_concurrency();

        let dispatch_spawner = op.dispatch_spawner(runtime_handle);
        let spawned_dispatch_result = dispatch_spawner.spawn_dispatch(
            vec![counting_receiver],
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
                Self::spawn_workers(
                    op.clone(),
                    spawned_dispatch_result.worker_receivers,
                    &mut task_set,
                    runtime_stats.clone(),
                    memory_manager.clone(),
                );

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
                    info_span!("BlockingSink::Finalize"),
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
