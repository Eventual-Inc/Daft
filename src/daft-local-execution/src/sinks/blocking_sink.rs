use std::{collections::VecDeque, pin::pin, sync::Arc};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use futures::StreamExt;
use snafu::ResultExt;
use tracing::{info_span, instrument};

use crate::{
    buffer::buffered_by_morsel_size,
    channel::{create_channel, Receiver},
    pipeline::PipelineNode,
    progress_bar::ProgressBarColor,
    resource_manager::MemoryManager,
    runtime_stats::{CountingReceiver, CountingSender, RuntimeStatsContext},
    ExecutionRuntimeContext, ExecutionTaskSpawner, JoinSnafu, OperatorOutput, OrderableJoinSet,
    TaskSet,
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
        let child_results_receiver = self.child.start(true, runtime_handle)?;
        let counting_receiver = CountingReceiver::new(
            child_results_receiver,
            self.runtime_stats.clone(),
            progress_bar.clone(),
        );

        let (destination_sender, destination_receiver) = create_channel(1);
        let counting_sender =
            CountingSender::new(destination_sender, self.runtime_stats.clone(), progress_bar);

        let op = self.op.clone();
        let spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("BlockingSink"),
        );
        runtime_handle.spawn(
            async move {
                let input_stream = pin!(counting_receiver.into_stream());

                let mut buffered_stream = buffered_by_morsel_size(input_stream, None);

                let mut pending_tasks = OrderableJoinSet::new(true);
                let mut states = (0..op.max_concurrency())
                    .map(|_| op.make_state())
                    .collect::<DaftResult<VecDeque<_>>>()?;

                loop {
                    let num_pending = pending_tasks.num_pending();
                    let can_spawn = !states.is_empty();
                    tokio::select! {
                        biased;
                        Some(new_input) = buffered_stream.next(), if can_spawn => {
                            let next_state = states.pop_front().unwrap();
                            pending_tasks.spawn(op.sink(new_input?, next_state, &spawner));
                        }
                        Some(finished_task) = pending_tasks.join_next(), if num_pending > 0 => {
                            let status = finished_task???;

                            match status {
                                BlockingSinkStatus::NeedMoreInput(state) => {
                                    states.push_back(state);
                                }
                                BlockingSinkStatus::Finished(state) => {
                                    states.push_back(state);
                                }
                            }
                        }
                        else => {
                            break;
                        }
                    }
                }

                let output = op.finalize(states.into(), &spawner).await??;
                if let Some(output) = output {
                    let _ = counting_sender.send(output).await;
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
