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

/// Trait for maintaining state across intermediate operator executions.
/// Each worker in a concurrent intermediate operator has its own state.
pub(crate) trait IntermediateOpState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Default state implementation for operators that don't need custom state.
struct DefaultIntermediateOperatorState {}

impl IntermediateOpState for DefaultIntermediateOperatorState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Result of executing an intermediate operator on input data.
pub(crate) enum IntermediateOperatorResult {
    /// Operator needs more input data. Optionally returns output data.
    NeedMoreInput(Option<Arc<MicroPartition>>),
    /// Operator has more output to produce from the same input.
    HasMoreOutput {
        input: Arc<MicroPartition>,
        output: Arc<MicroPartition>,
    },
}

pub(crate) type IntermediateOpExecuteResult =
    OperatorOutput<DaftResult<(Box<dyn IntermediateOpState>, IntermediateOperatorResult)>>;

/// Trait defining the behavior of intermediate operators in the execution pipeline.
pub(crate) trait IntermediateOperator: Send + Sync {
    /// Execute the operator on input data with the given state.
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult;

    /// Get the name of this operator for display purposes.
    fn name(&self) -> &'static str;

    /// Get a multi-line display representation of this operator.
    fn multiline_display(&self) -> Vec<String>;

    /// Create initial state for this operator.
    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(DefaultIntermediateOperatorState {}))
    }

    /// The maximum number of concurrent workers that can be spawned for this operator.
    /// Each worker will have its own IntermediateOperatorState.
    /// This method should be overridden if the operator needs to limit the number of
    /// concurrent workers, e.g., UDFs with resource requests.
    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(get_compute_pool_num_threads())
    }

    /// Define the preferred morsel size range for this operator.
    /// By default, we don't specify a lower bound for the morsel size range, so the input stream
    /// will only be capped by the default morsel size.
    fn morsel_size_range(&self, runtime_handle: &ExecutionRuntimeContext) -> (usize, usize) {
        (0, runtime_handle.default_morsel_size())
    }
}

/// Pipeline node that wraps an intermediate operator.
pub struct IntermediateNode {
    intermediate_op: Arc<dyn IntermediateOperator>,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<RuntimeStatsContext>,
    plan_stats: StatsState,
}

impl IntermediateNode {
    pub(crate) fn new(
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<Box<dyn PipelineNode>>,
        plan_stats: StatsState,
    ) -> Self {
        let rts = RuntimeStatsContext::new(intermediate_op.name());
        Self::new_with_runtime_stats(intermediate_op, children, rts, plan_stats)
    }

    pub(crate) fn new_with_runtime_stats(
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<Box<dyn PipelineNode>>,
        runtime_stats: Arc<RuntimeStatsContext>,
        plan_stats: StatsState,
    ) -> Self {
        Self {
            intermediate_op,
            children,
            runtime_stats,
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
        let child_receivers = self
            .children
            .iter()
            .map(|child| child.start(maintain_order, runtime_handle))
            .collect::<crate::Result<Vec<_>>>()?;

        // Flatten all child streams into a single input stream
        let input_stream = stream::iter(
            child_receivers
                .into_iter()
                .map(|receiver| receiver.into_stream()),
        )
        .flatten();

        // Apply morsel size buffering based on operator preferences
        let (min_morsel_size, max_morsel_size) =
            self.intermediate_op.morsel_size_range(runtime_handle);
        let buffered_input_stream =
            buffer_by_morsel_range(input_stream, min_morsel_size, max_morsel_size);

        let counting_input_stream = CountingStream::new(
            buffered_input_stream,
            self.runtime_stats.clone(),
            progress_bar,
        );

        Ok(counting_input_stream)
    }

    fn setup_output_channel(
        &self,
        progress_bar: Option<Arc<OperatorProgressBar>>,
    ) -> crate::Result<(CountingSender, Receiver<Arc<MicroPartition>>)> {
        let (output_sender, output_receiver) = create_channel(1);
        let counted_output_sender =
            CountingSender::new(output_sender, self.runtime_stats.clone(), progress_bar);
        Ok((counted_output_sender, output_receiver))
    }

    /// Run the intermediate operator with concurrent worker management.
    async fn run_operator(
        op: Arc<dyn IntermediateOperator>,
        mut input_stream: impl Stream<Item = DaftResult<Arc<MicroPartition>>> + Unpin,
        output_sender: CountingSender,
        task_spawner: ExecutionTaskSpawner,
        maintain_order: bool,
    ) -> DaftResult<()> {
        let mut operator_states = (0..op.max_concurrency()?)
            .map(|_| op.make_state())
            .collect::<DaftResult<VecDeque<_>>>()?;
        let mut joinset = OrderableJoinSet::new(maintain_order);

        loop {
            let has_available_state = !operator_states.is_empty();
            tokio::select! {
                biased;
                // Process new input if we have available worker states
                Some(input) = input_stream.next(), if has_available_state => {
                    let state = operator_states.pop_front().unwrap();
                    joinset.spawn(
                        op.execute(input?, state, &task_spawner)
                    );
                }
                // Handle completed worker tasks
                Some(result) = joinset.join_next() => {
                    let (state, result) = result???;
                    match result {
                        IntermediateOperatorResult::NeedMoreInput(Some(mp)) => {
                            // Worker finished and produced output, make state available again
                            operator_states.push_back(state);
                            if output_sender.send(mp).await.is_err() {
                                break;
                            }
                        }
                        IntermediateOperatorResult::NeedMoreInput(None) => {
                            // Worker finished without output, make state available again
                            operator_states.push_back(state);
                        }
                        IntermediateOperatorResult::HasMoreOutput { input, output } => {
                            // Worker has more output to produce from the same input
                            if output_sender.send(output).await.is_err() {
                                break;
                            }
                            // Re-submit the same input with the same state
                            joinset.spawn_front(op.execute(input, state, &task_spawner));
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }
        Ok(())
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

    /// Start the intermediate operator pipeline node.
    ///
    /// This method orchestrates the setup and execution of the operator pipeline.
    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        // Setup progress bar
        let progress_bar = runtime_handle.make_progress_bar(
            self.name(),
            ProgressBarColor::Magenta,
            true,
            self.runtime_stats.clone(),
        );

        // Setup input stream processing
        let counted_input_stream =
            self.setup_input_stream(maintain_order, runtime_handle, progress_bar.clone())?;

        // Setup output channel
        let (counted_output_sender, output_receiver) = self.setup_output_channel(progress_bar)?;

        // Create task spawner for compute tasks
        let task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            Span::current(),
        );

        // Spawn the operator
        runtime_handle.spawn(
            IntermediateNode::run_operator(
                self.intermediate_op.clone(),
                counted_input_stream,
                counted_output_sender,
                task_spawner,
                maintain_order,
            ),
            self.name(),
        );

        Ok(output_receiver)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
