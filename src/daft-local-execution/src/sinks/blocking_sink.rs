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
pub trait BlockingSinkState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub(crate) type BlockingSinkSinkResult = OperatorOutput<DaftResult<Box<dyn BlockingSinkState>>>;
pub(crate) type BlockingSinkFinalizeResult =
    OperatorOutput<DaftResult<Option<Arc<MicroPartition>>>>;
pub trait BlockingSink: Send + Sync {
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn BlockingSinkState>,
        spawner: &ComputeTaskSpawner,
    ) -> BlockingSinkSinkResult;
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ComputeTaskSpawner,
    ) -> BlockingSinkFinalizeResult;
    fn name(&self) -> &'static str;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>>;
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
    fn maintain_order(&self) -> bool {
        false
    }
}

struct BlockingSinkWorker {
    op: Arc<dyn BlockingSink>,
    state: Option<Box<dyn BlockingSinkState>>,
    task_spawner: ComputeTaskSpawner,
}

impl BlockingSinkWorker {
    pub fn try_new(
        op: Arc<dyn BlockingSink>,
        task_spawner: ComputeTaskSpawner,
    ) -> DaftResult<Self> {
        let state = op.make_state()?;
        Ok(Self {
            op,
            state: Some(state),
            task_spawner,
        })
    }

    pub async fn sink(
        mut worker: BlockingSinkWorker,
        morsel: Arc<MicroPartition>,
    ) -> DaftResult<BlockingSinkWorker> {
        let state = worker.state.take().unwrap();
        let state = worker
            .op
            .sink(morsel, state, &worker.task_spawner)
            .await??;
        worker.state = Some(state);
        Ok(worker)
    }

    pub async fn finalize(
        workers: VecDeque<BlockingSinkWorker>,
        op: Arc<dyn BlockingSink>,
        task_spawner: ComputeTaskSpawner,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let states = workers
            .into_iter()
            .map(|mut w| w.state.take().unwrap())
            .collect();
        let result = op.finalize(states, &task_spawner).await??;
        Ok(result)
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
            runtime_stats: RuntimeStatsContext::new(),
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
        let child_results_receiver = self.child.start(false, runtime_handle)?;
        let counting_receiver = CountingReceiver::new(
            child_results_receiver,
            self.runtime_stats.clone(),
            progress_bar.clone(),
        );

        let (destination_sender, destination_receiver) = create_channel(0);
        let counting_sender =
            CountingSender::new(destination_sender, self.runtime_stats.clone(), progress_bar);

        let memory_manager = runtime_handle.memory_manager.clone();
        let runtime_stats = self.runtime_stats.clone();
        let workers = (0..self.op.max_concurrency())
            .map(|_| {
                let task_spawner = ComputeTaskSpawner::new(
                    get_compute_runtime(),
                    memory_manager.clone(),
                    runtime_stats.clone(),
                    info_span!("BlockingSink::sink"),
                );
                BlockingSinkWorker::try_new(self.op.clone(), task_spawner)
            })
            .collect::<DaftResult<VecDeque<_>>>();
        let op = self.op.clone();
        let maintain_order = self.op.maintain_order();
        runtime_handle.spawn(
            async move {
                let mut workers = workers?;
                let mut worker_spawner = LocalTaskSpawner::new(maintain_order);
                loop {
                    let in_progress_len = worker_spawner.len();
                    tokio::select! {
                        // Bias currently executing workers over new input
                        biased;
                        // Process new input
                        Some(morsel) = counting_receiver.recv(), if in_progress_len < op.max_concurrency() => {
                            let worker = workers.pop_front().unwrap();
                            worker_spawner.push_back(BlockingSinkWorker::sink(worker, morsel));
                        },
                        // Process results from currently executing workers
                        Some(worker) = worker_spawner.next(), if in_progress_len > 0 => {
                            let worker = worker.map_err(|e| DaftError::InternalError(format!("Error joining next task: {:?}", e)))??;
                            workers.push_back(worker);
                        },

                        // No more input and workers are done processing
                        else  => {
                            break;
                        }
                    }
                }
                assert!(worker_spawner.len() == 0, "Worker spawner should be empty: {:?}, op: {:?}", worker_spawner.len(), op.name());
                let task_spawner = ComputeTaskSpawner::new(
                    get_compute_runtime(),
                    memory_manager,
                    runtime_stats,
                    info_span!("BlockingSink::finalize"),
                );
                let result = BlockingSinkWorker::finalize(workers, op.clone(), task_spawner).await?;
                if let Some(result) = result {
                    let _ = counting_sender.send(result).await;
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
