use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_runtime::JoinSet;
use tokio::sync::oneshot;

use crate::{
    ExecutionTaskSpawner,
    channel::{Receiver, Sender, create_channel},
    join::join_operator::JoinOperator,
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::{RuntimeStats, RuntimeStatsManagerHandle},
};

/// Slot for one input_id: either probe will receive (sender stored) or build already sent (value stored).
enum BuildStateSlot<T> {
    Sender(oneshot::Sender<T>),
    Ready(T),
}

/// Result of subscribing for finalized build state: either await the receiver or use the ready value.
pub(crate) enum FinalizedBuildStateReceiver<Op: JoinOperator> {
    Receiver(oneshot::Receiver<Op::FinalizedBuildState>),
    Ready(Op::FinalizedBuildState),
}

/// Bridge for communicating finalized build state between build and probe sides.
/// Uses oneshot: one send per input_id; probe receives once.
pub(crate) struct BuildStateBridge<Op: JoinOperator> {
    channels: Mutex<HashMap<InputId, BuildStateSlot<Op::FinalizedBuildState>>>,
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
        if let Some(slot) = channels.remove(&input_id) {
            if let BuildStateSlot::Sender(tx) = slot {
                let _ = tx.send(finalized);
            }
        } else {
            channels.insert(input_id, BuildStateSlot::Ready(finalized));
        }
    }

    pub(crate) fn subscribe(&self, input_id: InputId) -> FinalizedBuildStateReceiver<Op> {
        let mut channels = self.channels.lock().unwrap();
        let (tx, rx) = oneshot::channel();
        match channels.entry(input_id) {
            Entry::Vacant(e) => {
                e.insert(BuildStateSlot::Sender(tx));
                FinalizedBuildStateReceiver::Receiver(rx)
            }
            Entry::Occupied(e) => {
                let slot = e.remove();
                match slot {
                    BuildStateSlot::Ready(v) => FinalizedBuildStateReceiver::Ready(v),
                    BuildStateSlot::Sender(_) => {
                        channels.insert(input_id, BuildStateSlot::Sender(tx));
                        FinalizedBuildStateReceiver::Receiver(rx)
                    }
                }
            }
        }
    }
}

/// Process all morsels for a single input_id on the build side, then finalize.
async fn process_single_input<Op: JoinOperator + 'static>(
    input_id: InputId,
    mut receiver: Receiver<PipelineMessage>,
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    build_state_bridge: Arc<BuildStateBridge<Op>>,
    runtime_stats: Arc<dyn RuntimeStats>,
) -> DaftResult<()> {
    let mut state = op.make_build_state()?;

    while let Some(msg) = receiver.recv().await {
        match msg {
            PipelineMessage::Morsel { partition, .. } => {
                runtime_stats.add_rows_in(partition.len() as u64);
                state = op.build(partition, state, &task_spawner).await??;
            }
            PipelineMessage::Flush(_) => {
                break;
            }
        }
    }

    let finalized = op.finalize_build(state)?;
    build_state_bridge.send_finalized_build_state(input_id, finalized);
    Ok(())
}

pub(crate) struct BuildExecutionContext<Op: JoinOperator> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    build_state_bridge: Arc<BuildStateBridge<Op>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    stats_manager: RuntimeStatsManagerHandle,
    node_id: usize,
}

impl<Op: JoinOperator + 'static> BuildExecutionContext<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
        build_state_bridge: Arc<BuildStateBridge<Op>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        stats_manager: RuntimeStatsManagerHandle,
        node_id: usize,
    ) -> Self {
        Self {
            op,
            task_spawner,
            build_state_bridge,
            runtime_stats,
            stats_manager,
            node_id,
        }
    }

    pub(crate) async fn process_build_input(
        &self,
        receiver: Receiver<PipelineMessage>,
    ) -> DaftResult<()> {
        let mut receiver = receiver;
        let mut per_input_senders: HashMap<InputId, Sender<PipelineMessage>> = HashMap::new();
        let mut processor_set: JoinSet<DaftResult<()>> = JoinSet::new();
        let mut node_initialized = false;
        let mut input_closed = false;

        while !input_closed || !processor_set.is_empty() {
            tokio::select! {
                msg = receiver.recv(), if !input_closed => {
                    let Some(msg) = msg else {
                        input_closed = true;
                        per_input_senders.clear();
                        continue;
                    };

                    if !node_initialized {
                        self.stats_manager.activate_node(self.node_id);
                        node_initialized = true;
                    }

                    let input_id = match &msg {
                        PipelineMessage::Morsel { input_id, .. } => *input_id,
                        PipelineMessage::Flush(input_id) => *input_id,
                    };

                    if let Entry::Vacant(e) = per_input_senders.entry(input_id) {
                        let (tx, rx) = create_channel(1);
                        e.insert(tx);

                        let op = self.op.clone();
                        let task_spawner = self.task_spawner.clone();
                        let build_state_bridge = self.build_state_bridge.clone();
                        let runtime_stats = self.runtime_stats.clone();
                        processor_set.spawn(async move {
                            process_single_input(
                                input_id, rx, op, task_spawner,
                                build_state_bridge, runtime_stats,
                            )
                            .await
                        });
                    }

                    let is_flush = matches!(&msg, PipelineMessage::Flush(_));
                    if per_input_senders[&input_id].send(msg).await.is_err() {
                        // Processor died — error will surface from join below
                    }
                    if is_flush {
                        per_input_senders.remove(&input_id);
                    }
                }
                Some(result) = processor_set.join_next(), if !processor_set.is_empty() => {
                    result??;
                }
            }
        }
        Ok(())
    }
}
