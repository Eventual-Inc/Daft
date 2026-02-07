use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Instant,
};

use common_error::DaftResult;
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads};
use daft_micropartition::MicroPartition;
use tokio::sync::watch;

use crate::{
    ExecutionTaskSpawner,
    channel::Receiver,
    join::join_operator::JoinOperator,
    pipeline_execution::{InputStateTracker, PipelineEvent, StateTracker, next_event},
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{RuntimeStats, RuntimeStatsManagerHandle},
};

/// Bridge for communicating finalized build state between build and probe sides.
/// Uses `watch` channels so that late subscribers still see the last sent value.
pub(crate) struct BuildStateBridge<Op: JoinOperator> {
    channels: Mutex<HashMap<InputId, watch::Sender<Option<Op::FinalizedBuildState>>>>,
}

impl<Op: JoinOperator> BuildStateBridge<Op> {
    pub(crate) fn new() -> Self {
        Self {
            channels: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn send_finalized_build_state(
        &self,
        input_id: InputId,
        finalized: Op::FinalizedBuildState,
    ) {
        let mut channels = self.channels.lock().unwrap();
        let sender = channels.entry(input_id).or_insert_with(|| {
            let (sender, _receiver) = watch::channel(None);
            sender
        });
        sender.send_modify(|v| *v = Some(finalized));
    }

    pub(crate) fn subscribe(
        &self,
        input_id: InputId,
    ) -> watch::Receiver<Option<Op::FinalizedBuildState>> {
        let mut channels = self.channels.lock().unwrap();
        let sender = channels.entry(input_id).or_insert_with(|| {
            let (sender, _receiver) = watch::channel(None);
            sender
        });
        let receiver = sender.subscribe();
        dbg!("BRIDGE subscribe", input_id, receiver.borrow().is_some());
        receiver
    }
}

/// Per-node task result for build side.
/// Finalize tasks return `None` since they produce no output.
struct BuildTaskResult<S> {
    input_id: InputId,
    state: S,
    elapsed: std::time::Duration,
}

pub(crate) struct BuildExecutionContext<Op: JoinOperator> {
    task_set: OrderingAwareJoinSet<DaftResult<Option<BuildTaskResult<Op::BuildState>>>>,
    max_concurrency: usize,
    input_state_tracker: InputStateTracker<Op::BuildState>,
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    build_state_bridge: Arc<BuildStateBridge<Op>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    stats_manager: RuntimeStatsManagerHandle,
    node_id: usize,
    node_initialized: bool,
}

/// input_id lifecycle (no downstream flush):
///
/// 1. BUFFER   — morsel arrives, data buffered
/// 2. EXECUTE  — tasks spawned while batch_size met AND under max_concurrency
/// 3. FLUSH_IN — flush received → mark completed → pop_all allowed for remaining buffer
/// 4. FINALIZE — all tasks done + buffer empty → spawn finalize
/// 5. CLEANUP  — finalize done → remove input_id → send via BuildStateBridge
impl<Op: JoinOperator + 'static> BuildExecutionContext<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
        build_state_bridge: Arc<BuildStateBridge<Op>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        stats_manager: RuntimeStatsManagerHandle,
        node_id: usize,
    ) -> Self {
        let max_concurrency = get_compute_pool_num_threads();
        let task_set = OrderingAwareJoinSet::new(false);

        let op_for_state_creator = op.clone();
        let input_state_tracker = InputStateTracker::new(Box::new(
            move |_input_id| -> DaftResult<StateTracker<Op::BuildState>> {
                let state = op_for_state_creator.make_build_state()?;
                Ok(StateTracker::new(
                    vec![state],
                    0,
                    NonZeroUsize::new(usize::MAX).unwrap(),
                ))
            },
        ));

        Self {
            task_set,
            max_concurrency,
            input_state_tracker,
            op,
            task_spawner,
            build_state_bridge,
            runtime_stats,
            stats_manager,
            node_id,
            node_initialized: false,
        }
    }

    fn spawn_build_task(
        &mut self,
        partition: Arc<MicroPartition>,
        state: Op::BuildState,
        input_id: InputId,
    ) {
        let op = self.op.clone();
        let task_spawner = self.task_spawner.clone();
        self.task_set.spawn(async move {
            let now = Instant::now();
            let new_state = op.build(partition, state, &task_spawner).await??;
            let elapsed = now.elapsed();
            Ok(Some(BuildTaskResult {
                input_id,
                state: new_state,
                elapsed,
            }))
        });
    }

    fn try_spawn_tasks_for_input(&mut self, input_id: InputId) -> DaftResult<()> {
        while self.task_set.len() < self.max_concurrency {
            let Some(next) = self
                .input_state_tracker
                .get_next_partition_for_execute(input_id)
            else {
                break;
            };
            let (partition, state) = next?;
            self.spawn_build_task(partition, state, input_id);
        }
        Ok(())
    }

    fn try_spawn_tasks_all_inputs(&mut self) -> DaftResult<()> {
        let input_ids = self.input_state_tracker.input_ids();
        for input_id in input_ids {
            if self.task_set.len() >= self.max_concurrency {
                break;
            }
            self.try_spawn_tasks_for_input(input_id)?;
            self.try_spawn_finalize_task(input_id)?;
        }
        Ok(())
    }

    fn try_spawn_finalize_task(&mut self, input_id: InputId) -> DaftResult<bool> {
        if let Some(states) = self.input_state_tracker.try_take_states_for_finalize(input_id) {
            let op = self.op.clone();
            let build_state_bridge = self.build_state_bridge.clone();
            self.task_set.spawn(async move {
                let state = states.into_iter().next().expect("Should have state");
                let finalized = op.finalize_build(state)?;
                build_state_bridge.send_finalized_build_state(input_id, finalized);
                Ok(None)
            });
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn handle_build_completed(
        &mut self,
        input_id: InputId,
        state: Op::BuildState,
        elapsed: std::time::Duration,
    ) -> DaftResult<()> {
        self.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);
        self.input_state_tracker.return_state(input_id, state);
        self.try_spawn_tasks_all_inputs()?;
        Ok(())
    }

    fn handle_morsel(
        &mut self,
        input_id: InputId,
        partition: Arc<MicroPartition>,
    ) -> DaftResult<()> {
        if !self.node_initialized {
            self.stats_manager.activate_node(self.node_id);
            self.node_initialized = true;
        }
        self.runtime_stats.add_rows_in(partition.len() as u64);
        self.input_state_tracker
            .buffer_partition(input_id, partition)?;
        self.try_spawn_tasks_all_inputs()?;
        Ok(())
    }

    fn handle_flush(&mut self, input_id: InputId) -> DaftResult<()> {
        if !self.input_state_tracker.contains_key(input_id) {
            return Ok(());
        }
        self.input_state_tracker.mark_completed(input_id);
        self.try_spawn_tasks_for_input(input_id)?;
        self.try_spawn_finalize_task(input_id)?;
        Ok(())
    }

    fn handle_input_closed(&mut self) -> DaftResult<()> {
        self.input_state_tracker.mark_all_completed();
        self.try_spawn_tasks_all_inputs()?;
        Ok(())
    }

    pub(crate) async fn process_build_input(
        &mut self,
        receiver: Receiver<PipelineMessage>,
    ) -> DaftResult<()> {
        let mut receiver = receiver;
        let mut input_closed = false;

        while !input_closed || !self.task_set.is_empty() || !self.input_state_tracker.is_empty() {
            dbg!("BUILD loop top", input_closed, self.task_set.len(), self.input_state_tracker.is_empty());
            let event = next_event(
                &mut self.task_set,
                self.max_concurrency,
                &mut receiver,
                &mut input_closed,
            )
            .await?;
            match event {
                PipelineEvent::TaskCompleted(Some(BuildTaskResult {
                    input_id,
                    state,
                    elapsed,
                })) => {
                    dbg!("BUILD TaskCompleted(Some)", input_id);
                    self.handle_build_completed(input_id, state, elapsed)?;
                }
                PipelineEvent::TaskCompleted(None) => {
                    dbg!("BUILD TaskCompleted(None) - finalize done");
                }
                PipelineEvent::Morsel {
                    input_id,
                    partition,
                } => {
                    dbg!("BUILD Morsel", input_id, partition.len());
                    self.handle_morsel(input_id, partition)?;
                }
                PipelineEvent::Flush(input_id) => {
                    dbg!("BUILD Flush", input_id);
                    self.handle_flush(input_id)?;
                }
                PipelineEvent::InputClosed => {
                    dbg!("BUILD InputClosed");
                    self.handle_input_closed()?;
                }
            }
        }
        dbg!("BUILD loop done");
        Ok(())
    }
}
