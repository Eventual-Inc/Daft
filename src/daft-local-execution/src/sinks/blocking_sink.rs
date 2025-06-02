use std::{collections::VecDeque, sync::Arc};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime, OrderableJoinSet};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use futures::{Stream, StreamExt};
use tracing::Span;

use crate::{
    buffer::buffer_by_morsel_range,
    channel::{create_channel, Receiver},
    pipeline::PipelineNode,
    progress_bar::{OperatorProgressBar, ProgressBarColor},
    runtime_stats::{CountingSender, CountingStream, RuntimeStatsContext},
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput,
};

/// Trait for maintaining state across blocking sink executions.
/// Each worker in a concurrent blocking sink has its own state.
pub(crate) trait BlockingSinkState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Result of processing input data in a blocking sink.
pub(crate) enum BlockingSinkStatus {
    /// Sink needs more input data and the state can be reused.
    NeedMoreInput(Box<dyn BlockingSinkState>),
    /// Sink has finished processing and the state should be collected for finalization.
    #[allow(dead_code)]
    Finished(Box<dyn BlockingSinkState>),
}

pub(super) type BlockingSinkSinkResult = OperatorOutput<DaftResult<BlockingSinkStatus>>;
pub(super) type BlockingSinkFinalizeResult =
    OperatorOutput<DaftResult<Option<Arc<MicroPartition>>>>;

/// Trait defining the behavior of blocking sinks in the execution pipeline.
/// Blocking sinks consume all input data before producing final output.
pub(crate) trait BlockingSink: Send + Sync {
    /// Process input data with the given state.
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult;

    /// Finalize processing by combining all worker states and producing final output.
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult;

    /// Get the name of this sink for display purposes.
    fn name(&self) -> &'static str;

    /// Get a multi-line display representation of this sink.
    fn multiline_display(&self) -> Vec<String>;

    /// Create initial state for this sink.
    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>>;

    /// Define the preferred morsel size range for this sink.
    /// By default, we use a fixed morsel size range of [default_morsel_size, default_morsel_size] for
    /// blocking sinks.
    fn morsel_size_range(&self, runtime_handle: &ExecutionRuntimeContext) -> (usize, usize) {
        (
            runtime_handle.default_morsel_size(),
            runtime_handle.default_morsel_size(),
        )
    }

    /// The maximum number of concurrent workers that can be spawned for this sink.
    /// Each worker will have its own BlockingSinkState.
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
}

/// Pipeline node that wraps a blocking sink operator.
pub(crate) struct BlockingSinkNode {
    op: Arc<dyn BlockingSink>,
    name: &'static str,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<RuntimeStatsContext>,
    plan_stats: StatsState,
}

impl BlockingSinkNode {
    pub fn new(
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

    pub fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    /// Create and configure the input stream from the child node.
    fn setup_input_stream(
        &self,
        runtime_handle: &mut ExecutionRuntimeContext,
        progress_bar: Option<Arc<OperatorProgressBar>>,
    ) -> crate::Result<impl Stream<Item = DaftResult<Arc<MicroPartition>>> + Unpin> {
        // Start the child node
        let input_receiver = self.child.start(false, runtime_handle)?;

        // Apply morsel size buffering based on sink preferences
        let (lower_bound, upper_bound) = self.op.morsel_size_range(runtime_handle);
        let buffered_input_stream =
            buffer_by_morsel_range(input_receiver.into_stream(), lower_bound, upper_bound);

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

    /// Run the blocking sink with concurrent worker management.
    async fn run_sink(
        bs: Arc<dyn BlockingSink>,
        mut input_stream: impl Stream<Item = DaftResult<Arc<MicroPartition>>> + Unpin,
        output_sender: CountingSender,
        task_spawner: ExecutionTaskSpawner,
    ) -> DaftResult<()> {
        // Initialize worker states
        let mut worker_states = (0..bs.max_concurrency())
            .map(|_| bs.make_state())
            .collect::<DaftResult<VecDeque<_>>>()?;

        let mut joinset = OrderableJoinSet::new(false);

        loop {
            let has_available_state = !worker_states.is_empty();

            tokio::select! {
                biased;

                // Process new input if we have available worker states
                Some(input) = input_stream.next(), if has_available_state => {
                    let state = worker_states.pop_front().unwrap();
                    joinset.spawn(bs.sink(input?, state, &task_spawner));
                }

                // Handle completed worker tasks
                Some(result) = joinset.join_next() => {
                    let result = result???;
                    match result {
                        BlockingSinkStatus::NeedMoreInput(state) => {
                            // Worker finished and is ready for more input
                            worker_states.push_back(state);
                        }
                        BlockingSinkStatus::Finished(state) => {
                            // Worker finished processing
                            worker_states.push_back(state);
                        }
                    }
                }

                // If the input stream is exhausted and all workers are finished, break the loop
                else => {
                    break;
                }
            }
        }

        // Finalize all worker states and produce final output
        let finalized_result = bs.finalize(worker_states.into(), &task_spawner).await??;
        if let Some(res) = finalized_result {
            let _ = output_sender.send(res).await;
        }

        Ok(())
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

    /// Start the blocking sink pipeline node.
    ///
    /// This method orchestrates the setup and execution of the sink pipeline.
    fn start(
        &self,
        _maintain_order: bool,
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
        let counting_stream = self.setup_input_stream(runtime_handle, progress_bar.clone())?;

        // Setup output channel
        let (counting_sender, output_receiver) = self.setup_output_channel(progress_bar)?;

        // Create task spawner for compute tasks
        let task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            Span::current(),
        );

        // Spawn the sink operator
        runtime_handle.spawn(
            BlockingSinkNode::run_sink(
                self.op.clone(),
                counting_stream,
                counting_sender,
                task_spawner,
            ),
            self.name(),
        );

        Ok(output_receiver)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
