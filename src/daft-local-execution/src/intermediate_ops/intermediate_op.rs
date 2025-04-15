use std::{collections::VecDeque, sync::Arc};

use common_display::tree::TreeDisplay;
use common_error::{DaftError, DaftResult};
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use crate::{
    channel::{create_channel, Receiver},
    pipeline::PipelineNode,
    progress_bar::ProgressBarColor,
    runtime_stats::{CountingReceiver, CountingSender, RuntimeStatsContext},
    spawner::{ComputeTaskSpawner, LocalTaskSpawner},
    ExecutionRuntimeContext, OperatorOutput,
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
    // Operator is done processing input and needs more input
    // The Option<Arc<MicroPartition>> is the result of the previous input
    NeedMoreInput(Option<Arc<MicroPartition>>),
    // Operator is not done processing the current input, and has more output to emit
    // The first Arc<MicroPartition> is the input that is being processed
    // The second Arc<MicroPartition> is the output that is being emitted
    HasMoreOutput(Arc<MicroPartition>, Arc<MicroPartition>),
}

pub(crate) type IntermediateOpExecuteResult =
    OperatorOutput<DaftResult<(Box<dyn IntermediateOpState>, IntermediateOperatorResult)>>;
pub trait IntermediateOperator: Send + Sync {
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        task_spawner: &ComputeTaskSpawner,
    ) -> IntermediateOpExecuteResult;
    fn name(&self) -> &'static str;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(DefaultIntermediateOperatorState {}))
    }
    /// The maximum number of concurrent workers that can be spawned for this operator.
    /// Each worker will has its own IntermediateOperatorState.
    /// This method should be overridden if the operator needs to limit the number of concurrent workers, i.e. UDFs with resource requests.
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn morsel_size(&self, runtime_handle: &ExecutionRuntimeContext) -> Option<usize> {
        Some(runtime_handle.default_morsel_size())
    }
}

struct IntermediateOpWorker {
    op: Arc<dyn IntermediateOperator>,
    state: Option<Box<dyn IntermediateOpState>>,
    task_spawner: ComputeTaskSpawner,
}

impl IntermediateOpWorker {
    pub fn try_new(
        op: Arc<dyn IntermediateOperator>,
        task_spawner: ComputeTaskSpawner,
    ) -> DaftResult<Self> {
        let state = op.make_state()?;
        Ok(Self {
            op,
            state: Some(state),
            task_spawner,
        })
    }

    #[instrument(level = "info", skip_all, name = "IntermediateOpWorker::execute")]
    pub async fn execute(
        mut worker: IntermediateOpWorker,
        morsel: Arc<MicroPartition>,
    ) -> DaftResult<(IntermediateOperatorResult, IntermediateOpWorker)> {
        let state = worker.state.take().unwrap();
        let result = worker
            .op
            .execute(morsel, state, &worker.task_spawner)
            .await??;
        worker.state = Some(result.0);
        Ok((result.1, worker))
    }
}

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
        let rts = RuntimeStatsContext::new();
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

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
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

    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        let progress_bar = runtime_handle.make_progress_bar(
            self.name(),
            ProgressBarColor::Magenta,
            true,
            self.runtime_stats.clone(),
        );
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

        let workers = (0..self.intermediate_op.max_concurrency())
            .map(|_| {
                let task_spawner = ComputeTaskSpawner::new(
                    get_compute_runtime(),
                    runtime_handle.memory_manager.clone(),
                    self.runtime_stats.clone(),
                    info_span!("IntermediateOp::execute"),
                );
                IntermediateOpWorker::try_new(self.intermediate_op.clone(), task_spawner)
            })
            .collect::<DaftResult<VecDeque<_>>>();

        let name = self.intermediate_op.name();
        let max_concurrency = self.intermediate_op.max_concurrency();
        runtime_handle.spawn(
            async move {
                let mut workers = workers?;
                let mut worker_spawner = LocalTaskSpawner::new(maintain_order);
                for receiver in child_result_receivers {
                    loop {
                        let in_progress_len = worker_spawner.len();
                        tokio::select! {
                            // Bias new input over currently executing workers
                            biased;
                            // Process new input
                            Some(morsel) = receiver.recv(), if in_progress_len < max_concurrency => {
                                let worker = workers.pop_front().unwrap();
                                worker_spawner.push_back(IntermediateOpWorker::execute(worker, morsel));
                            },
                            // Process results from currently executing workers
                            Some(result) = worker_spawner.next(), if in_progress_len > 0 => {
                                let (result, worker) = result.map_err(|e| DaftError::InternalError(format!("Error joining next task: {:?}", e)))??;

                                match result {
                                    IntermediateOperatorResult::NeedMoreInput(Some(mp)) => {
                                        workers.push_back(worker);
                                        if let Err(_) = counting_sender.send(mp).await {
                                            break;
                                        }
                                    }
                                    IntermediateOperatorResult::NeedMoreInput(None) => {
                                        workers.push_back(worker);
                                    }
                                    IntermediateOperatorResult::HasMoreOutput(input_mp, output_mp) => {
                                        if let Err(_) = counting_sender.send(output_mp).await {
                                            break;
                                        }
                                        // Push the worker back to the front of the queue to maintain order, if needed
                                        worker_spawner.push_front(IntermediateOpWorker::execute(worker, input_mp));
                                    }
                                }
                            },
                            // No more input and workers are done processing
                            else => break
                        }
                    }
                    assert!(worker_spawner.len() == 0);
                }
                Ok(())
            },
            name,
        );
        Ok(destination_receiver)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
