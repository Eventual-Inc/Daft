use std::sync::Arc;

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use crate::{
    channel::{create_channel, Receiver},
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    ops::{NodeCategory, NodeInfo, NodeType},
    pipeline::{NodeName, PipelineNode, RuntimeContext},
    resource_manager::MemoryManager,
    runtime_stats::{
        CountingSender, DefaultRuntimeStats, InitializingCountingReceiver, RuntimeStats,
    },
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput, TaskSet,
};

pub enum BlockingSinkStatus<Op: BlockingSink> {
    NeedMoreInput(Op::State),
    #[allow(dead_code)]
    Finished(Op::State),
}

pub enum BlockingSinkFinalizeOutput<Op: BlockingSink> {
    #[allow(dead_code)]
    HasMoreOutput {
        states: Vec<Op::State>,
        output: Vec<Arc<MicroPartition>>,
    },
    Finished(Vec<Arc<MicroPartition>>),
}

pub(crate) type BlockingSinkSinkResult<Op> = OperatorOutput<DaftResult<BlockingSinkStatus<Op>>>;
pub(crate) type BlockingSinkFinalizeResult<Op> =
    OperatorOutput<DaftResult<BlockingSinkFinalizeOutput<Op>>>;
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
    ) -> BlockingSinkFinalizeResult<Self>
    where
        Self: Sized;
    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> DaftResult<Self::State>;
    fn make_runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        Arc::new(DefaultRuntimeStats::default())
    }
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
    ) -> Self {
        let name = op.name().into();
        let node_info = ctx.next_node_info(name, op.op_type(), NodeCategory::BlockingSink);
        let runtime_stats = op.make_runtime_stats();

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

    #[instrument(level = "info", skip_all, name = "BlockingSink::run_worker")]
    async fn run_worker(
        op: Arc<Op>,
        input_receiver: Receiver<Arc<MicroPartition>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        memory_manager: Arc<MemoryManager>,
    ) -> DaftResult<Op::State> {
        let span = info_span!("BlockingSink::Sink");
        let compute_runtime = get_compute_runtime();
        let spawner =
            ExecutionTaskSpawner::new(compute_runtime, memory_manager, runtime_stats, span);
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
        op: Arc<Op>,
        input_receivers: Vec<Receiver<Arc<MicroPartition>>>,
        task_set: &mut TaskSet<DaftResult<Op::State>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        memory_manager: Arc<MemoryManager>,
    ) {
        for input_receiver in input_receivers {
            task_set.spawn(Self::run_worker(
                op.clone(),
                input_receiver,
                runtime_stats.clone(),
                memory_manager.clone(),
            ));
        }
    }
}

impl<Op: BlockingSink> TreeDisplay for BlockingSinkNode<Op> {
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
                    for (name, value) in rt_result {
                        writeln!(display, "{} = {}", name.capitalize(), value).unwrap();
                    }
                }
            }
        }
        display
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

    fn start(
        &self,
        _maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let child_results_receiver = self.child.start(false, runtime_handle)?;
        let counting_receiver = InitializingCountingReceiver::new(
            child_results_receiver,
            self.node_id(),
            self.runtime_stats.clone(),
            runtime_handle.stats_manager(),
        );

        let (destination_sender, destination_receiver) = create_channel(0);
        let counting_sender = CountingSender::new(destination_sender, self.runtime_stats.clone());

        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let num_workers = op.max_concurrency();

        let dispatch_spawner = op.dispatch_spawner(runtime_handle);
        let spawned_dispatch_result = dispatch_spawner.spawn_dispatch(
            vec![counting_receiver],
            num_workers,
            &mut runtime_handle.handle(),
        );
        runtime_handle.spawn_local(
            async move { spawned_dispatch_result.spawned_dispatch_task.await? },
            &self.name(),
        );

        let memory_manager = runtime_handle.memory_manager();
        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();
        runtime_handle.spawn_local(
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
                    let state = result??;
                    finished_states.push(state);
                }

                let compute_runtime = get_compute_runtime();
                let spawner = ExecutionTaskSpawner::new(
                    compute_runtime,
                    memory_manager,
                    runtime_stats.clone(),
                    info_span!("BlockingSink::Finalize"),
                );
                loop {
                    let finalized_result = op.finalize(finished_states, &spawner).await??;
                    match finalized_result {
                        BlockingSinkFinalizeOutput::HasMoreOutput { states, output } => {
                            for output in output {
                                let _ = counting_sender.send(output).await;
                            }
                            finished_states = states;
                        }
                        BlockingSinkFinalizeOutput::Finished(output) => {
                            for output in output {
                                let _ = counting_sender.send(output).await;
                            }
                            break;
                        }
                    }
                }

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
