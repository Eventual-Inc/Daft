use std::{
    collections::{HashMap, VecDeque, hash_map::Entry},
    sync::{Arc, Mutex},
};

use common_error::{DaftError, DaftResult};
use common_metrics::{Meter, ops::NodeInfo};
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads};
use daft_micropartition::MicroPartition;
use tokio::sync::oneshot;

use crate::{
    ExecutionTaskSpawner,
    channel::Receiver,
    join::{join_operator::JoinOperator, stats::JoinStats},
    pipeline::{InputId, PipelineEvent, PipelineMessage, next_event},
    runtime_stats::{RuntimeStats, RuntimeStatsManagerHandle},
};

enum BuildStateSlot<T> {
    // The channel carries a `DaftResult` so a build-side failure can be delivered to a waiting
    // probe (via `poison`) instead of leaving it blocked forever.
    Sender(oneshot::Sender<DaftResult<T>>),
    // Successfully finalized build state, waiting to be picked up by a (later) subscriber.
    Ready(T),
}

struct BridgeState<T> {
    channels: HashMap<InputId, BuildStateSlot<T>>,
    /// Set when the build side fails. Once poisoned, every current and future subscriber receives
    /// this error rather than waiting indefinitely for a finalized build state that never arrives.
    poisoned: Option<String>,
}

pub(crate) enum FinalizedBuildStateReceiver<Op: JoinOperator> {
    Receiver(oneshot::Receiver<DaftResult<Op::FinalizedBuildState>>),
    Ready(DaftResult<Op::FinalizedBuildState>),
}

pub(crate) struct BuildStateBridge<Op: JoinOperator> {
    state: Mutex<BridgeState<Op::FinalizedBuildState>>,
}

impl<Op: JoinOperator> BuildStateBridge<Op> {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(BridgeState {
                channels: HashMap::new(),
                poisoned: None,
            }),
        }
    }

    pub(crate) fn send_finalized_build_state(
        &self,
        input_id: InputId,
        finalized: Op::FinalizedBuildState,
    ) {
        let mut state = self.state.lock().unwrap();
        if state.poisoned.is_some() {
            return;
        }
        if let Some(slot) = state.channels.remove(&input_id) {
            if let BuildStateSlot::Sender(tx) = slot {
                let _ = tx.send(Ok(finalized));
            }
        } else {
            state.channels.insert(input_id, BuildStateSlot::Ready(finalized));
        }
    }

    /// Mark the bridge as failed: deliver `msg` to every currently-waiting subscriber and to any
    /// future subscriber, so the probe side never blocks forever on a build that won't finalize.
    pub(crate) fn poison(&self, msg: String) {
        let mut state = self.state.lock().unwrap();
        if state.poisoned.is_some() {
            return;
        }
        state.poisoned = Some(msg.clone());
        for (_, slot) in state.channels.drain() {
            if let BuildStateSlot::Sender(tx) = slot {
                let _ = tx.send(Err(DaftError::ComputeError(msg.clone())));
            }
        }
    }

    pub(crate) fn subscribe(&self, input_id: InputId) -> FinalizedBuildStateReceiver<Op> {
        let mut state = self.state.lock().unwrap();
        if let Some(msg) = &state.poisoned {
            return FinalizedBuildStateReceiver::Ready(Err(DaftError::ComputeError(msg.clone())));
        }
        let (tx, rx) = oneshot::channel();
        match state.channels.entry(input_id) {
            Entry::Vacant(e) => {
                e.insert(BuildStateSlot::Sender(tx));
                FinalizedBuildStateReceiver::Receiver(rx)
            }
            Entry::Occupied(e) => {
                let slot = e.remove();
                match slot {
                    BuildStateSlot::Ready(v) => FinalizedBuildStateReceiver::Ready(Ok(v)),
                    BuildStateSlot::Sender(_) => {
                        state.channels.insert(input_id, BuildStateSlot::Sender(tx));
                        FinalizedBuildStateReceiver::Receiver(rx)
                    }
                }
            }
        }
    }
}

type BuildTaskResult<Op> = DaftResult<(InputId, <Op as JoinOperator>::BuildState)>;

struct PerBuildInput<Op: JoinOperator> {
    state: Option<Op::BuildState>,
    pending: VecDeque<MicroPartition>,
    flushed: bool,
    runtime_stats: Arc<JoinStats>,
}

impl<Op: JoinOperator + 'static> PerBuildInput<Op> {
    fn new(state: Op::BuildState, runtime_stats: Arc<JoinStats>) -> Self {
        Self {
            state: Some(state),
            pending: VecDeque::new(),
            flushed: false,
            runtime_stats,
        }
    }

    /// If the state is idle and there is pending work, concat all pending
    /// partitions and spawn a single build task.
    fn flush_pending(
        &mut self,
        tasks: &mut OrderingAwareJoinSet<BuildTaskResult<Op>>,
        op: &Arc<Op>,
        spawner: &ExecutionTaskSpawner,
        input_id: InputId,
    ) -> DaftResult<()> {
        if self.pending.is_empty() || self.state.is_none() {
            return Ok(());
        }
        let state = self.state.take().unwrap();
        let partition = if self.pending.len() == 1 {
            self.pending.pop_front().unwrap()
        } else {
            MicroPartition::concat(self.pending.drain(..).collect::<Vec<_>>())?
        };
        let op = op.clone();
        let spawner = spawner.clone();
        tasks.spawn(async move {
            let state = op.build(partition, state, &spawner).await??;
            Ok((input_id, state))
        });
        Ok(())
    }

    fn is_idle(&self) -> bool {
        self.state.is_some()
    }

    fn ready_to_finalize(&self) -> bool {
        self.flushed && self.is_idle()
    }
}

pub(crate) struct BuildExecutionContext<Op: JoinOperator> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    build_state_bridge: Arc<BuildStateBridge<Op>>,
    stats_manager: RuntimeStatsManagerHandle,
    node_id: usize,
    meter: Meter,
    node_info: Arc<NodeInfo>,
}

impl<Op: JoinOperator + 'static> BuildExecutionContext<Op> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
        build_state_bridge: Arc<BuildStateBridge<Op>>,
        stats_manager: RuntimeStatsManagerHandle,
        node_id: usize,
        meter: Meter,
        node_info: Arc<NodeInfo>,
    ) -> Self {
        Self {
            op,
            task_spawner,
            build_state_bridge,
            stats_manager,
            node_id,
            meter,
            node_info,
        }
    }

    fn try_finalize(&self, per_input: PerBuildInput<Op>, input_id: InputId) -> DaftResult<()> {
        let state = per_input.state.expect("must be idle when finalizing");
        // Propagate a finalize failure: the caller returns it from `process_build_input`, which
        // poisons the bridge so the probe side is unblocked and the error surfaces to the consumer
        // (rather than silently hanging on a build state that will never arrive).
        let finalized = self.op.finalize_build(state)?;
        self.build_state_bridge
            .send_finalized_build_state(input_id, finalized);
        Ok(())
    }

    /// Drive the build side. On any failure, poison the build-state bridge so the concurrently
    /// running probe side stops waiting for a finalized build state and the error propagates to the
    /// consumer instead of deadlocking.
    pub(crate) async fn process_build_input(
        &self,
        receiver: Receiver<PipelineMessage>,
    ) -> DaftResult<()> {
        let result = self.process_build_input_inner(receiver).await;
        if let Err(e) = &result {
            self.build_state_bridge
                .poison(format!("hash join build side failed: {e}"));
        }
        result
    }

    async fn process_build_input_inner(
        &self,
        receiver: Receiver<PipelineMessage>,
    ) -> DaftResult<()> {
        let mut receiver = receiver;
        let mut inputs: HashMap<InputId, PerBuildInput<Op>> = HashMap::new();
        let mut tasks: OrderingAwareJoinSet<BuildTaskResult<Op>> = OrderingAwareJoinSet::new(false);
        let mut node_initialized = false;
        let mut child_closed = false;

        while let Some(event) = next_event(
            &mut tasks,
            get_compute_pool_num_threads(),
            &mut receiver,
            &mut child_closed,
        )
        .await?
        {
            match event {
                PipelineEvent::TaskCompleted((input_id, state)) => {
                    let per_input = inputs.get_mut(&input_id).unwrap();
                    per_input.state = Some(state);
                    per_input.flush_pending(&mut tasks, &self.op, &self.task_spawner, input_id)?;

                    if inputs.get(&input_id).is_some_and(|p| p.ready_to_finalize()) {
                        self.try_finalize(inputs.remove(&input_id).unwrap(), input_id)?;
                    }
                }
                PipelineEvent::Morsel {
                    input_id,
                    partition,
                } => {
                    if !node_initialized {
                        self.stats_manager.activate_node(self.node_id);
                        node_initialized = true;
                    }

                    let per_input = match inputs.entry(input_id) {
                        Entry::Occupied(e) => e.into_mut(),
                        Entry::Vacant(e) => {
                            let runtime_stats =
                                Arc::new(JoinStats::new(&self.meter, &self.node_info));
                            self.stats_manager.register_runtime_stats(
                                self.node_id,
                                input_id,
                                runtime_stats.clone(),
                            );
                            let state = self.op.make_build_state()?;
                            e.insert(PerBuildInput::new(state, runtime_stats))
                        }
                    };
                    per_input
                        .runtime_stats
                        .add_build_rows_inserted(partition.len() as u64);
                    per_input
                        .runtime_stats
                        .add_build_bytes_inserted(partition.size_bytes() as u64);
                    per_input.pending.push_back(partition);
                    per_input.flush_pending(&mut tasks, &self.op, &self.task_spawner, input_id)?;
                }
                PipelineEvent::Flush(input_id) => {
                    if let Some(p) = inputs.get_mut(&input_id) {
                        p.flushed = true;
                    }
                    if inputs.get(&input_id).is_some_and(|p| p.ready_to_finalize()) {
                        self.try_finalize(inputs.remove(&input_id).unwrap(), input_id)?;
                    }
                }
                PipelineEvent::FlightPartitionRef => {
                    unreachable!(
                        "BuildExecutionContext should not receive flight partition refs from child"
                    )
                }
                PipelineEvent::InputClosed => {
                    for p in inputs.values_mut() {
                        p.flushed = true;
                    }
                    let ready_ids: Vec<_> = inputs
                        .iter()
                        .filter(|(_, p)| p.ready_to_finalize())
                        .map(|(id, _)| *id)
                        .collect();
                    for input_id in ready_ids {
                        let per_input = inputs.remove(&input_id).unwrap();
                        self.try_finalize(per_input, input_id)?;
                    }
                }
            }
        }
        Ok(())
    }
}
