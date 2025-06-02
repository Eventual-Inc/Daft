use std::{collections::VecDeque, sync::Arc};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime, OrderableJoinSet};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use futures::{stream, Stream, StreamExt};
use tracing::Span;

use crate::{
    buffer::buffer_by_morsel_range,
    channel::{create_channel, Receiver},
    pipeline::PipelineNode,
    progress_bar::{OperatorProgressBar, ProgressBarColor},
    runtime_stats::{CountingSender, CountingStream, RuntimeStatsContext},
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
};

/// Trait for maintaining state across streaming sink executions.
/// Each worker in a concurrent streaming sink has its own state.
pub(crate) trait StreamingSinkState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Result of executing a streaming sink on input data.
pub(crate) enum StreamingSinkOutput {
    /// Sink needs more input data. Optionally returns output data.
    NeedMoreInput(Option<Arc<MicroPartition>>),
    /// Sink has more output to produce from the same input.
    #[allow(dead_code)]
    HasMoreOutput {
        input: Arc<MicroPartition>,
        output: Arc<MicroPartition>,
    },
    /// Sink has finished processing.
    Finished(Option<Arc<MicroPartition>>),
}

pub(crate) type StreamingSinkExecuteResult =
    OperatorOutput<DaftResult<(Box<dyn StreamingSinkState>, StreamingSinkOutput)>>;
pub(crate) type StreamingSinkFinalizeResult =
    OperatorOutput<DaftResult<Option<Arc<MicroPartition>>>>;

/// Trait defining the behavior of streaming sinks in the execution pipeline.
/// Streaming sinks can produce output incrementally as they process input data.
pub(crate) trait StreamingSink: Send + Sync {
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

    /// Get a multi-line display representation of this sink.
    fn multiline_display(&self) -> Vec<String>;

    /// Create a new worker-local state for this StreamingSink.
    fn make_state(&self) -> Box<dyn StreamingSinkState>;

    /// The maximum number of concurrent workers that can be spawned for this sink.
    /// Each worker will has its own StreamingSinkState.
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    /// By default, we don't specify a lower bound for the morsel size range, so the input stream
    /// will only be capped by the default morsel size.
    fn morsel_size_range(&self, runtime_handle: &ExecutionRuntimeContext) -> (usize, usize) {
        (0, runtime_handle.default_morsel_size())
    }
}

/// Pipeline node that wraps a streaming sink operator.
pub struct StreamingSinkNode {
    sink: Arc<dyn StreamingSink>,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<RuntimeStatsContext>,
    plan_stats: StatsState,
}

impl StreamingSinkNode {
    pub fn new(
        sink: Arc<dyn StreamingSink>,
        children: Vec<Box<dyn PipelineNode>>,
        plan_stats: StatsState,
    ) -> Self {
        let name = sink.name();
        Self {
            sink,
            children,
            runtime_stats: RuntimeStatsContext::new(name),
            plan_stats,
        }
    }

    pub fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    /// Create and configure the input stream from child nodes.
    fn setup_input_stream(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
        progress_bar: Option<Arc<OperatorProgressBar>>,
    ) -> crate::Result<impl Stream<Item = DaftResult<Arc<MicroPartition>>> + Unpin> {
        // Start all child nodes and collect their output receivers
        let input_receivers = self
            .children
            .iter()
            .map(|child| child.start(maintain_order, runtime_handle))
            .collect::<crate::Result<Vec<_>>>()?;

        // Flatten all child streams into a single input stream
        let input_stream = stream::iter(
            input_receivers
                .into_iter()
                .map(|receiver| receiver.into_stream()),
        )
        .flatten();

        // Apply morsel size buffering based on sink preferences
        let (lower_bound, upper_bound) = self.sink.morsel_size_range(runtime_handle);
        let buffered_input_stream = buffer_by_morsel_range(input_stream, lower_bound, upper_bound);

        let counting_stream = CountingStream::new(
            buffered_input_stream,
            self.runtime_stats.clone(),
            progress_bar,
        );

        Ok(counting_stream)
    }

    /// Setup the output channel for the sink.
    fn setup_output_channel(
        &self,
        progress_bar: Option<Arc<OperatorProgressBar>>,
    ) -> crate::Result<(CountingSender, Receiver<Arc<MicroPartition>>)> {
        let (destination_sender, destination_receiver) = create_channel(1);
        let counting_sender =
            CountingSender::new(destination_sender, self.runtime_stats.clone(), progress_bar);
        Ok((counting_sender, destination_receiver))
    }

    /// Run the streaming sink with concurrent worker management.
    async fn run_sink(
        sink: Arc<dyn StreamingSink>,
        mut input_stream: impl Stream<Item = DaftResult<Arc<MicroPartition>>> + Unpin,
        output_sender: CountingSender,
        task_spawner: ExecutionTaskSpawner,
    ) -> DaftResult<()> {
        // Initialize worker states
        let mut worker_states = (0..sink.max_concurrency())
            .map(|_| sink.make_state())
            .collect::<VecDeque<_>>();

        let mut joinset = OrderableJoinSet::new(false);

        loop {
            let has_available_state = !worker_states.is_empty();

            tokio::select! {
                biased;

                // Process new input if we have available worker states
                Some(input) = input_stream.next(), if has_available_state => {
                    let state = worker_states.pop_front().unwrap();
                    joinset.spawn(sink.execute(input?, state, &task_spawner));
                }

                // Handle completed worker tasks
                Some(result) = joinset.join_next() => {
                    let (state, output) = result???;
                    match output {
                        StreamingSinkOutput::NeedMoreInput(mp) => {
                            // Worker finished and optionally produced output, make state available again
                            worker_states.push_back(state);
                            if let Some(mp) = mp {
                                if output_sender.send(mp).await.is_err() {
                                    break;
                                }
                            }
                        }
                        StreamingSinkOutput::HasMoreOutput { input, output } => {
                            // Worker has more output to produce from the same input
                            if output_sender.send(output).await.is_err() {
                                break;
                            }
                            // Re-submit the same input with the same state
                            joinset.spawn_front(sink.execute(input, state, &task_spawner));
                        }
                        StreamingSinkOutput::Finished(mp) => {
                            // Worker finished processing
                            if let Some(mp) = mp {
                                if output_sender.send(mp).await.is_err() {
                                    break;
                                }
                            }
                            break;
                        }
                    }
                }

                else => {
                    break;
                }
            }
        }

        // Finalize all worker states and produce final output
        let finalized_result = sink.finalize(worker_states.into(), &task_spawner).await??;
        if let Some(res) = finalized_result {
            let _ = output_sender.send(res).await;
        }

        Ok(())
    }
}

impl TreeDisplay for StreamingSinkNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        use common_display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.sink.name()).unwrap();
            }
            level => {
                let multiline_display = self.sink.multiline_display().join("\n");
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
        self.children.iter().map(|v| v.as_tree_display()).collect()
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
        self.sink.name()
    }

    /// Start the streaming sink pipeline node.
    ///
    /// This method orchestrates the setup and execution of the sink pipeline.
    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        // Setup progress bar
        let progress_bar = runtime_handle.make_progress_bar(
            self.name(),
            ProgressBarColor::Cyan,
            true,
            self.runtime_stats.clone(),
        );

        // Setup input stream processing
        let counting_stream =
            self.setup_input_stream(maintain_order, runtime_handle, progress_bar.clone())?;

        // Setup output channel
        let (counting_sender, destination_receiver) = self.setup_output_channel(progress_bar)?;

        // Create task spawner for compute tasks
        let task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            Span::current(),
        );

        // Spawn the sink operator
        runtime_handle.spawn(
            Self::run_sink(
                self.sink.clone(),
                counting_stream,
                counting_sender,
                task_spawner,
            ),
            self.name(),
        );

        Ok(destination_receiver)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
