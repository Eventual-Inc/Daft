use std::{collections::VecDeque, sync::Arc};

use common_display::tree::TreeDisplay;
use common_error::{DaftError, DaftResult};
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use tracing::info_span;

use crate::{
    channel::{create_channel, Receiver},
    pipeline::PipelineNode,
    progress_bar::ProgressBarColor,
    runtime_stats::{CountingReceiver, CountingSender, RuntimeStatsContext},
    spawner::{ComputeTaskSpawner, LocalTaskSpawner},
    ExecutionRuntimeContext, OperatorOutput,
};

pub trait StreamingSinkState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub enum StreamingSinkOutput {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput((Arc<MicroPartition>, Arc<MicroPartition>)),
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
        spawner: &ComputeTaskSpawner,
    ) -> StreamingSinkExecuteResult;

    /// Finalize the StreamingSink operator, with the given states from each worker.
    fn finalize(
        &self,
        states: Vec<Box<dyn StreamingSinkState>>,
        spawner: &ComputeTaskSpawner,
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

struct StreamingSinkWorker {
    op: Arc<dyn StreamingSink>,
    state: Box<dyn StreamingSinkState>,
    task_spawner: ComputeTaskSpawner,
}

impl StreamingSinkWorker {
    pub fn new(op: Arc<dyn StreamingSink>, task_spawner: ComputeTaskSpawner) -> Self {
        let state = op.make_state();
        Self {
            op,
            state,
            task_spawner,
        }
    }

    pub async fn execute(
        mut worker: Self,
        morsel: Arc<MicroPartition>,
    ) -> DaftResult<(StreamingSinkOutput, Self)> {
        let (state, result) = worker
            .op
            .execute(morsel, worker.state, &worker.task_spawner)
            .await??;
        worker.state = state;
        Ok((result, worker))
    }

    pub async fn finalize(
        workers: VecDeque<Self>,
        op: Arc<dyn StreamingSink>,
        task_spawner: ComputeTaskSpawner,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let states = workers.into_iter().map(|w| w.state).collect();
        let result = op.finalize(states, &task_spawner).await??;
        Ok(result)
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
            runtime_stats: RuntimeStatsContext::new(),
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

        let (destination_sender, destination_receiver) = create_channel(0);
        let counting_sender =
            CountingSender::new(destination_sender, self.runtime_stats.clone(), progress_bar);

        let op = self.op.clone();
        let max_concurrency = op.max_concurrency();
        let runtime_stats = self.runtime_stats.clone();
        let memory_manager = runtime_handle.memory_manager();

        let mut workers = (0..max_concurrency)
            .map(|_| {
                let task_spawner = ComputeTaskSpawner::new(
                    get_compute_runtime(),
                    memory_manager.clone(),
                    runtime_stats.clone(),
                    info_span!("StreamingSink::execute"),
                );
                StreamingSinkWorker::new(self.op.clone(), task_spawner)
            })
            .collect::<VecDeque<_>>();
        runtime_handle.spawn(
            async move {
                let mut worker_spawner = LocalTaskSpawner::new(maintain_order);
                let mut finished = false;
                for receiver in child_result_receivers {
                    if finished {
                        break;
                    }
                    loop {
                        let in_progress_len = worker_spawner.len();
                        tokio::select! {
                            // Bias currently executing workers over new input
                            biased;
                            // Process new input
                            Some(morsel) = receiver.recv(), if in_progress_len < max_concurrency => {
                                let worker = workers.pop_front().unwrap();
                                worker_spawner.push_back(StreamingSinkWorker::execute(worker, morsel));
                            },
                            // Process results from currently executing workers
                            Some(result) = worker_spawner.next(), if in_progress_len > 0 => {
                                let (result, worker) = result.map_err(|e| DaftError::InternalError(format!("Error joining next task: {:?}", e)))??;

                                match result {
                                    StreamingSinkOutput::NeedMoreInput(Some(mp)) => {
                                        workers.push_back(worker);
                                        if let Err(_) = counting_sender.send(mp).await {
                                            finished = true;
                                            break;
                                        }
                                    }
                                    StreamingSinkOutput::NeedMoreInput(None) => {
                                        workers.push_back(worker);
                                    }
                                    StreamingSinkOutput::HasMoreOutput((result_mp, input_mp)) => {
                                        if let Err(_) = counting_sender.send(result_mp).await {
                                            finished = true;
                                            break;
                                        }
                                        // Push the worker back to the front of the queue to maintain order, if needed
                                        worker_spawner.push_front(StreamingSinkWorker::execute(worker, input_mp));
                                    }
                                    StreamingSinkOutput::Finished(Some(mp)) => {
                                        if let Err(_) = counting_sender.send(mp).await {
                                            finished = true;
                                            break;
                                        }
                                    }
                                    StreamingSinkOutput::Finished(None) => {
                                        finished = true;
                                        break;
                                    }
                                }
                            },
                            // No more input and workers are done processing
                            else => break
                        }
                    }
                    assert!(worker_spawner.len() == 0);
                }
                if finished {
                    return Ok(());
                }

                let compute_runtime = get_compute_runtime();
                let spawner = ComputeTaskSpawner::new(
                    compute_runtime,
                    memory_manager,
                    runtime_stats.clone(),
                    info_span!("StreamingSink::Finalize"),
                );

                let finalized_result = StreamingSinkWorker::finalize(workers, op.clone(), spawner).await?;
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
