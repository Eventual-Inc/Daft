use std::{collections::VecDeque, pin::pin, sync::Arc};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use futures::StreamExt;
use tracing::info_span;

use crate::{
    buffer::buffered_by_morsel_size,
    channel::{create_channel, Receiver},
    pipeline::PipelineNode,
    progress_bar::ProgressBarColor,
    runtime_stats::{CountingReceiver, CountingSender, RuntimeStatsContext},
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput, OrderableJoinSet,
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

        let (destination_sender, destination_receiver) = create_channel(1);

        let op = self.op.clone();
        let task_spawner = ExecutionTaskSpawner {
            runtime_ref: get_compute_runtime(),
            memory_manager: runtime_handle.memory_manager(),
            runtime_context: self.runtime_stats.clone(),
            outer_span: info_span!("streaming_sink"),
        };
        let progress_bar = progress_bar.clone();
        let stats = self.runtime_stats.clone();
        runtime_handle.spawn(
            async move {
                let flattened_stream = pin!(futures::stream::iter(
                    child_result_receivers.into_iter().map(|v| v.into_stream()),
                )
                .flatten());

                let mut buffered_stream = buffered_by_morsel_size(flattened_stream, None);

                let mut pending_tasks = OrderableJoinSet::new(maintain_order);
                let mut states = (0..op.max_concurrency())
                    .map(|_| op.make_state())
                    .collect::<VecDeque<_>>();
                let sender = destination_sender.into_inner();

                loop {
                    let num_pending = pending_tasks.num_pending();
                    let can_spawn = !states.is_empty();
                    let output_task = async {
                        let mut should_break = false;
                        if let Ok(permit) = sender.reserve().await {
                            let finished_task =
                                pending_tasks.join_next().await.expect("No finished task");
                            let (state, result) = finished_task???;
                            match result {
                                StreamingSinkOutput::NeedMoreInput(Some(output)) => {
                                    states.push_back(state);
                                    stats.mark_rows_emitted(output.len() as u64);
                                    if let Some(ref pb) = progress_bar {
                                        pb.render();
                                    }
                                    permit.send(output);
                                }
                                StreamingSinkOutput::NeedMoreInput(None) => {
                                    states.push_back(state);
                                }
                                StreamingSinkOutput::Finished(Some(output)) => {
                                    stats.mark_rows_emitted(output.len() as u64);
                                    if let Some(ref pb) = progress_bar {
                                        pb.render();
                                    }
                                    permit.send(output);
                                    should_break = true;
                                }
                                StreamingSinkOutput::Finished(None) => {
                                    states.push_back(state);
                                    should_break = true;
                                }
                                StreamingSinkOutput::HasMoreOutput(output) => {
                                    pending_tasks.spawn_first(op.execute(
                                        output.clone(),
                                        state,
                                        &task_spawner,
                                    ));
                                    stats.mark_rows_emitted(output.len() as u64);
                                    if let Some(ref pb) = progress_bar {
                                        pb.render();
                                    }
                                    permit.send(output);
                                }
                            }
                        } else {
                            should_break = true;
                        }
                        DaftResult::Ok(should_break)
                    };
                    tokio::select! {
                        biased;
                        Some(new_input) = buffered_stream.next(), if can_spawn => {
                            let input = new_input?;
                            let next_state = states.pop_front().unwrap();
                            pending_tasks.spawn(op.execute(input, next_state, &task_spawner));
                        }
                        result = output_task , if num_pending > 0 => {
                            let should_break = result?;
                            if should_break {
                                return Ok(());
                            }
                        }
                        else => {
                            break;
                        }
                    }
                }
                assert!(buffered_stream.next().await.is_none());
                assert!(pending_tasks.num_pending() == 0);

                let final_output = op.finalize(states.into(), &task_spawner).await??;
                if let Some(output) = final_output {
                    if sender.send(output).await.is_err() {
                        return Ok(());
                    }
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
