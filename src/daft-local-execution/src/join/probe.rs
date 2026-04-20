use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
    time::{Duration, Instant},
};

use common_error::DaftResult;
use common_metrics::{Meter, ops::NodeInfo};
use common_runtime::OrderingAwareJoinSet;

use crate::{
    ExecutionTaskSpawner,
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender},
    join::{
        build::{BuildStateBridge, FinalizedBuildStateReceiver},
        join_operator::{JoinOperator, ProbeOutput},
        stats::JoinStats,
    },
    pipeline::{InputId, PipelineEvent, PipelineMessage, next_event},
    runtime_stats::{RuntimeStats, RuntimeStatsManagerHandle},
};

enum TaskOutput<Op: JoinOperator> {
    BuildStateReady {
        input_id: InputId,
        finalized: Op::FinalizedBuildState,
    },
    ProbeComplete {
        input_id: InputId,
        state: Op::ProbeState,
        output: ProbeOutput,
        elapsed: Duration,
    },
    Completed,
}

type TaskResult<Op> = DaftResult<TaskOutput<Op>>;

struct PerProbeInput<Op: JoinOperator> {
    states: Vec<Op::ProbeState>,
    buffer: RowBasedBuffer,
    flushed: bool,
    runtime_stats: Arc<JoinStats>,
    max_concurrency: usize,
}

impl<Op: JoinOperator + 'static> PerProbeInput<Op> {
    fn new(op: &Op, runtime_stats: Arc<JoinStats>, max_concurrency: usize) -> Self {
        let (lower, upper) = op.morsel_size_requirement().unwrap_or_default().values();
        Self {
            states: Vec::new(),
            buffer: RowBasedBuffer::new(lower, upper),
            flushed: false,
            runtime_stats,
            max_concurrency,
        }
    }

    fn spawn_ready_batches(
        &mut self,
        tasks: &mut OrderingAwareJoinSet<TaskResult<Op>>,
        op: &Arc<Op>,
        spawner: &ExecutionTaskSpawner,
        input_id: InputId,
    ) -> DaftResult<()> {
        while let Some(state) = self.states.pop() {
            if let Some(batch) = self.buffer.next_batch_if_ready()? {
                let op = op.clone();
                let spawner = spawner.clone();
                tasks.spawn(async move {
                    let now = Instant::now();
                    let (state, output) = op.probe(batch, state, &spawner).await??;
                    Ok(TaskOutput::ProbeComplete {
                        input_id,
                        state,
                        output,
                        elapsed: now.elapsed(),
                    })
                });
            } else {
                self.states.push(state);
                break;
            }
        }
        Ok(())
    }

    fn all_states_idle(&self) -> bool {
        self.states.len() == self.max_concurrency
    }

    fn ready_to_complete(&self) -> bool {
        self.flushed && self.all_states_idle()
    }
}

pub(crate) struct ProbeExecutionContext<Op: JoinOperator> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    finalize_spawner: ExecutionTaskSpawner,
    output_sender: Sender<PipelineMessage>,
    build_state_bridge: Arc<BuildStateBridge<Op>>,
    maintain_order: bool,
    stats_manager: RuntimeStatsManagerHandle,
    node_id: usize,
    meter: Meter,
    node_info: Arc<NodeInfo>,
}

impl<Op: JoinOperator + 'static> ProbeExecutionContext<Op> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
        finalize_spawner: ExecutionTaskSpawner,
        output_sender: Sender<PipelineMessage>,
        build_state_bridge: Arc<BuildStateBridge<Op>>,
        maintain_order: bool,
        stats_manager: RuntimeStatsManagerHandle,
        node_id: usize,
        meter: Meter,
        node_info: Arc<NodeInfo>,
    ) -> Self {
        Self {
            op,
            task_spawner,
            finalize_spawner,
            output_sender,
            build_state_bridge,
            maintain_order,
            stats_manager,
            node_id,
            meter,
            node_info,
        }
    }

    /// Drain remaining buffer, finalize, and send output downstream.
    fn spawn_complete_input(
        op: Arc<Op>,
        per_input: PerProbeInput<Op>,
        input_id: InputId,
        task_spawner: ExecutionTaskSpawner,
        finalize_spawner: ExecutionTaskSpawner,
        output_tx: Sender<PipelineMessage>,
        tasks: &mut OrderingAwareJoinSet<TaskResult<Op>>,
    ) {
        tasks.spawn(async move {
            let mut states = per_input.states;
            let mut buffer = per_input.buffer;
            let runtime_stats = per_input.runtime_stats;

            if let Some(mut partition) = buffer.pop_all()? {
                let mut state = states.pop().unwrap();
                loop {
                    let now = Instant::now();
                    let (new_state, result) = op.probe(partition, state, &task_spawner).await??;
                    let elapsed = now.elapsed();
                    runtime_stats.add_duration_us(elapsed.as_micros() as u64);

                    let output_mp = match &result {
                        ProbeOutput::NeedMoreInput(mp) => mp.as_ref(),
                        ProbeOutput::HasMoreOutput { output, .. } => Some(output),
                    };
                    if let Some(mp) = output_mp {
                        runtime_stats.add_probe_rows_out(mp.len() as u64);
                        runtime_stats.add_probe_bytes_out(mp.size_bytes() as u64);
                        let _ = output_tx
                            .send(PipelineMessage::Morsel {
                                input_id,
                                partition: mp.clone(),
                            })
                            .await;
                    }

                    match result {
                        ProbeOutput::NeedMoreInput(_) => {
                            states.push(new_state);
                            break;
                        }
                        ProbeOutput::HasMoreOutput { input, .. } => {
                            partition = input;
                            state = new_state;
                        }
                    }
                }
            }

            if op.needs_probe_finalization()
                && let Some(mp) = op.finalize_probe(states, &finalize_spawner).await??
            {
                runtime_stats.add_probe_rows_out(mp.len() as u64);
                runtime_stats.add_probe_bytes_out(mp.size_bytes() as u64);
                let _ = output_tx
                    .send(PipelineMessage::Morsel {
                        input_id,
                        partition: mp,
                    })
                    .await;
            }

            let _ = output_tx.send(PipelineMessage::Flush(input_id)).await;
            Ok(TaskOutput::Completed)
        });
    }

    pub(crate) async fn process_probe_input(
        &self,
        receiver: Receiver<PipelineMessage>,
    ) -> DaftResult<()> {
        let mut receiver = receiver;
        let max_concurrency = self.op.max_probe_concurrency();
        let mut inputs: HashMap<InputId, PerProbeInput<Op>> = HashMap::new();
        let mut tasks: OrderingAwareJoinSet<TaskResult<Op>> =
            OrderingAwareJoinSet::new(self.maintain_order);
        let mut child_closed = false;

        while let Some(event) = next_event(
            &mut tasks,
            max_concurrency,
            &mut receiver,
            &mut child_closed,
        )
        .await?
        {
            match event {
                PipelineEvent::TaskCompleted(TaskOutput::Completed) => {}
                PipelineEvent::TaskCompleted(TaskOutput::BuildStateReady {
                    input_id,
                    finalized,
                }) => {
                    let per_input = inputs.get_mut(&input_id).unwrap();
                    per_input.states = (0..max_concurrency)
                        .map(|_| self.op.make_probe_state(finalized.clone()))
                        .collect();
                    per_input.spawn_ready_batches(
                        &mut tasks,
                        &self.op,
                        &self.task_spawner,
                        input_id,
                    )?;

                    if inputs.get(&input_id).is_some_and(|p| p.ready_to_complete()) {
                        Self::spawn_complete_input(
                            self.op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            self.task_spawner.clone(),
                            self.finalize_spawner.clone(),
                            self.output_sender.clone(),
                            &mut tasks,
                        );
                    }
                }
                PipelineEvent::TaskCompleted(TaskOutput::ProbeComplete {
                    input_id,
                    state,
                    output,
                    elapsed,
                }) => {
                    let per_input = inputs.get_mut(&input_id).unwrap();
                    per_input
                        .runtime_stats
                        .add_duration_us(elapsed.as_micros() as u64);

                    let output_mp = match &output {
                        ProbeOutput::NeedMoreInput(mp) => mp.as_ref(),
                        ProbeOutput::HasMoreOutput { output, .. } => Some(output),
                    };
                    if let Some(mp) = output_mp {
                        per_input.runtime_stats.add_probe_rows_out(mp.len() as u64);
                        per_input
                            .runtime_stats
                            .add_probe_bytes_out(mp.size_bytes() as u64);
                        let _ = self
                            .output_sender
                            .send(PipelineMessage::Morsel {
                                input_id,
                                partition: mp.clone(),
                            })
                            .await;
                    }

                    match output {
                        ProbeOutput::NeedMoreInput(_) => {
                            per_input.states.push(state);
                            per_input.spawn_ready_batches(
                                &mut tasks,
                                &self.op,
                                &self.task_spawner,
                                input_id,
                            )?;

                            if inputs.get(&input_id).is_some_and(|p| p.ready_to_complete()) {
                                Self::spawn_complete_input(
                                    self.op.clone(),
                                    inputs.remove(&input_id).unwrap(),
                                    input_id,
                                    self.task_spawner.clone(),
                                    self.finalize_spawner.clone(),
                                    self.output_sender.clone(),
                                    &mut tasks,
                                );
                            }
                        }
                        ProbeOutput::HasMoreOutput { input, .. } => {
                            let op = self.op.clone();
                            let spawner = self.task_spawner.clone();
                            tasks.spawn(async move {
                                let now = Instant::now();
                                let (state, output) = op.probe(input, state, &spawner).await??;
                                Ok(TaskOutput::ProbeComplete {
                                    input_id,
                                    state,
                                    output,
                                    elapsed: now.elapsed(),
                                })
                            });
                        }
                    }
                }
                PipelineEvent::Morsel {
                    input_id,
                    partition,
                } => {
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

                            let bridge = self.build_state_bridge.clone();
                            tasks.spawn(async move {
                                let finalized = match bridge.subscribe(input_id) {
                                    FinalizedBuildStateReceiver::Receiver(rx) => {
                                        rx.await.map_err(|e| {
                                            common_error::DaftError::ValueError(format!(
                                                "Failed to receive finalized build state: {e}"
                                            ))
                                        })?
                                    }
                                    FinalizedBuildStateReceiver::Ready(v) => v,
                                };
                                Ok(TaskOutput::BuildStateReady {
                                    input_id,
                                    finalized,
                                })
                            });

                            e.insert(PerProbeInput::new(&self.op, runtime_stats, max_concurrency))
                        }
                    };
                    per_input
                        .runtime_stats
                        .add_probe_rows_in(partition.len() as u64);
                    per_input
                        .runtime_stats
                        .add_probe_bytes_in(partition.size_bytes() as u64);
                    per_input.buffer.push(partition);
                    per_input.spawn_ready_batches(
                        &mut tasks,
                        &self.op,
                        &self.task_spawner,
                        input_id,
                    )?;
                }
                PipelineEvent::Flush(input_id) => {
                    if let Some(p) = inputs.get_mut(&input_id) {
                        p.flushed = true;
                    }
                    if inputs.get(&input_id).is_some_and(|p| p.ready_to_complete()) {
                        Self::spawn_complete_input(
                            self.op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            self.task_spawner.clone(),
                            self.finalize_spawner.clone(),
                            self.output_sender.clone(),
                            &mut tasks,
                        );
                    }
                }
                PipelineEvent::ShuffleMetadata => {
                    unreachable!("Probe join should not receive shuffle metadata")
                }
                PipelineEvent::InputClosed => {
                    for p in inputs.values_mut() {
                        p.flushed = true;
                    }
                    let ready_ids: Vec<_> = inputs
                        .iter()
                        .filter(|(_, p)| p.ready_to_complete())
                        .map(|(id, _)| *id)
                        .collect();
                    for input_id in ready_ids {
                        Self::spawn_complete_input(
                            self.op.clone(),
                            inputs.remove(&input_id).unwrap(),
                            input_id,
                            self.task_spawner.clone(),
                            self.finalize_spawner.clone(),
                            self.output_sender.clone(),
                            &mut tasks,
                        );
                    }
                }
            }
        }
        Ok(())
    }
}
