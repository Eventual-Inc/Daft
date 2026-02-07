use std::{sync::Arc, time::Instant};

use common_error::DaftResult;
use common_runtime::{OrderingAwareJoinSet, get_compute_pool_num_threads};
use daft_micropartition::MicroPartition;

use crate::{
    ControlFlow, ExecutionTaskSpawner,
    channel::{Receiver, Sender},
    dynamic_batching::{BatchManager, BatchingStrategy},
    join::{
        build::BuildStateBridge,
        join_operator::{JoinOperator, ProbeOutput},
    },
    pipeline_execution::{InputStateTracker, PipelineEvent, StateTracker, next_event},
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::RuntimeStats,
};

/// Per-node task result for probe side.
enum ProbeTaskResult<S> {
    Probe {
        input_id: InputId,
        state: S,
        result: ProbeOutput,
        elapsed: std::time::Duration,
    },
    Finalize {
        input_id: InputId,
        output: Vec<Arc<MicroPartition>>,
    },
}

pub(crate) struct ProbeExecutionContext<Op: JoinOperator, Strategy: BatchingStrategy + 'static> {
    task_set: OrderingAwareJoinSet<DaftResult<ProbeTaskResult<Op::ProbeState>>>,
    max_concurrency: usize,
    input_state_tracker: InputStateTracker<Op::ProbeState>,
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    finalize_spawner: ExecutionTaskSpawner,
    output_sender: Sender<PipelineMessage>,
    batch_manager: Arc<BatchManager<Strategy>>,
    runtime_stats: Arc<dyn RuntimeStats>,
}

/// input_id lifecycle:
///
/// 1. BUFFER   — morsel arrives, data buffered
/// 2. EXECUTE  — tasks spawned while batch_size met AND under max_concurrency
/// 3. FLUSH_IN — flush received → mark completed → pop_all allowed for remaining buffer
/// 4. FINALIZE — all tasks done + buffer empty → spawn finalize
/// 5. CLEANUP  — finalize done → remove input_id → propagate flush downstream
impl<Op: JoinOperator + 'static, Strategy: BatchingStrategy + 'static>
    ProbeExecutionContext<Op, Strategy>
{
    pub(crate) fn new(
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
        finalize_spawner: ExecutionTaskSpawner,
        output_sender: Sender<PipelineMessage>,
        batch_manager: Arc<BatchManager<Strategy>>,
        build_state_bridge: Arc<BuildStateBridge<Op>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        maintain_order: bool,
    ) -> Self {
        let op_for_state_creator = op.clone();
        let build_state_bridge_for_state_creator = build_state_bridge.clone();
        let batch_manager_for_state_creator = batch_manager.clone();

        let state_creator = Box::new(
            move |input_id| -> DaftResult<StateTracker<Op::ProbeState>> {
                let max_probe_concurrency = op_for_state_creator.max_probe_concurrency();
                let probe_states: Vec<_> = (0..max_probe_concurrency)
                    .map(|_| {
                        let receiver = build_state_bridge_for_state_creator.subscribe(input_id);
                        op_for_state_creator.make_probe_state(receiver)
                    })
                    .collect();
                let (lower, upper) = batch_manager_for_state_creator
                    .calculate_batch_size()
                    .values();
                Ok(StateTracker::new(probe_states, lower, upper))
            },
        );
        let max_total_concurrency = get_compute_pool_num_threads();
        let task_set = OrderingAwareJoinSet::new(maintain_order);
        let input_state_tracker = InputStateTracker::new(state_creator);

        Self {
            task_set,
            max_concurrency: max_total_concurrency,
            input_state_tracker,
            op,
            task_spawner,
            finalize_spawner,
            output_sender,
            batch_manager,
            runtime_stats,
        }
    }

    #[inline]
    async fn send(&self, msg: PipelineMessage) -> ControlFlow {
        if self.output_sender.send(msg).await.is_err() {
            ControlFlow::Stop
        } else {
            ControlFlow::Continue
        }
    }

    fn spawn_probe_task(
        &mut self,
        partition: Arc<MicroPartition>,
        state: Op::ProbeState,
        input_id: InputId,
    ) {
        let op = self.op.clone();
        let task_spawner = self.task_spawner.clone();
        self.task_set.spawn(async move {
            let now = Instant::now();
            let (new_state, result) = op.probe(partition, state, &task_spawner).await??;
            let elapsed = now.elapsed();
            Ok(ProbeTaskResult::Probe {
                input_id,
                state: new_state,
                result,
                elapsed,
            })
        });
    }

    fn try_spawn_tasks_for_input(&mut self, input_id: InputId) -> DaftResult<()> {
        while self.task_set.len() < self.max_concurrency
            && let Some(next) = self
                .input_state_tracker
                .get_next_partition_for_execute(input_id)
        {
            let (partition, state) = next?;
            self.spawn_probe_task(partition, state, input_id);
        }
        Ok(())
    }

    fn try_spawn_tasks_all_inputs(&mut self) -> DaftResult<()> {
        let input_ids: Vec<InputId> = self.input_state_tracker.input_ids();
        for other_id in input_ids {
            if self.task_set.len() >= self.max_concurrency {
                break;
            }
            self.try_spawn_tasks_for_input(other_id)?;
            self.try_spawn_finalize_task(other_id)?;
        }
        Ok(())
    }

    fn try_spawn_finalize_task(&mut self, input_id: InputId) -> DaftResult<bool> {
        if let Some(states) = self
            .input_state_tracker
            .try_take_states_for_finalize(input_id)
        {
            let op = self.op.clone();
            let finalize_spawner = self.finalize_spawner.clone();
            self.task_set.spawn(async move {
                let output = op.finalize_probe(states, &finalize_spawner).await??;
                Ok(ProbeTaskResult::Finalize {
                    input_id,
                    output: output.into_iter().collect(),
                })
            });
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn handle_probe_completed(
        &mut self,
        input_id: InputId,
        state: Op::ProbeState,
        result: ProbeOutput,
        elapsed: std::time::Duration,
    ) -> DaftResult<ControlFlow> {
        self.runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

        // Send output if present
        if let Some(mp) = result.output() {
            self.runtime_stats.add_rows_out(mp.len() as u64);
            self.batch_manager.record_execution_stats(
                self.runtime_stats.as_ref(),
                mp.len(),
                elapsed,
            );
            if self
                .send(PipelineMessage::Morsel {
                    input_id,
                    partition: mp.clone(),
                })
                .await
                == ControlFlow::Stop
            {
                return Ok(ControlFlow::Stop);
            }
        }

        // Update batch bounds
        let new_requirements = self.batch_manager.calculate_batch_size();
        self.input_state_tracker.update_all_bounds(new_requirements);

        match result {
            ProbeOutput::NeedMoreInput(_) => {
                self.input_state_tracker.return_state(input_id, state);
                self.try_spawn_tasks_for_input(input_id)?;
                self.try_spawn_tasks_all_inputs()?;
                self.try_spawn_finalize_task(input_id)?;
                Ok(ControlFlow::Continue)
            }
            ProbeOutput::HasMoreOutput { input, .. } => {
                // Spawn another probe with same input and state
                let op = self.op.clone();
                let task_spawner = self.task_spawner.clone();
                self.task_set.spawn(async move {
                    let now = Instant::now();
                    let (new_state, result) = op.probe(input, state, &task_spawner).await??;
                    let elapsed = now.elapsed();
                    Ok(ProbeTaskResult::Probe {
                        input_id,
                        state: new_state,
                        result,
                        elapsed,
                    })
                });
                Ok(ControlFlow::Continue)
            }
        }
    }

    async fn handle_finalize_completed(
        &mut self,
        input_id: InputId,
        output: Vec<Arc<MicroPartition>>,
    ) -> DaftResult<ControlFlow> {
        for partition in output {
            self.runtime_stats.add_rows_out(partition.len() as u64);
            if self
                .send(PipelineMessage::Morsel {
                    input_id,
                    partition,
                })
                .await
                == ControlFlow::Stop
            {
                return Ok(ControlFlow::Stop);
            }
        }
        if self.send(PipelineMessage::Flush(input_id)).await == ControlFlow::Stop {
            return Ok(ControlFlow::Stop);
        }
        Ok(ControlFlow::Continue)
    }

    fn handle_morsel(
        &mut self,
        input_id: InputId,
        partition: Arc<MicroPartition>,
    ) -> DaftResult<()> {
        self.runtime_stats.add_rows_in(partition.len() as u64);
        self.input_state_tracker
            .buffer_partition(input_id, partition)?;
        self.try_spawn_tasks_all_inputs()?;
        Ok(())
    }

    async fn handle_flush(&mut self, input_id: InputId) -> DaftResult<ControlFlow> {
        if !self.input_state_tracker.contains_key(input_id) {
            if self.send(PipelineMessage::Flush(input_id)).await == ControlFlow::Stop {
                return Ok(ControlFlow::Stop);
            }
            return Ok(ControlFlow::Continue);
        }
        self.input_state_tracker.mark_completed(input_id);
        self.try_spawn_tasks_for_input(input_id)?;
        self.try_spawn_finalize_task(input_id)?;
        Ok(ControlFlow::Continue)
    }

    fn handle_input_closed(&mut self) -> DaftResult<()> {
        self.input_state_tracker.mark_all_completed();
        self.try_spawn_tasks_all_inputs()?;
        Ok(())
    }

    pub(crate) async fn process_probe_input(
        &mut self,
        receiver: Receiver<PipelineMessage>,
    ) -> DaftResult<()> {
        let mut receiver = receiver;
        let mut input_closed = false;

        while !input_closed || !self.task_set.is_empty() || !self.input_state_tracker.is_empty() {
            let event = next_event(
                &mut self.task_set,
                self.max_concurrency,
                &mut receiver,
                &mut input_closed,
            )
            .await?;
            let cf = match event {
                PipelineEvent::TaskCompleted(ProbeTaskResult::Probe {
                    input_id,
                    state,
                    result,
                    elapsed,
                }) => {
                    self.handle_probe_completed(input_id, state, result, elapsed)
                        .await?
                }
                PipelineEvent::TaskCompleted(ProbeTaskResult::Finalize { input_id, output }) => {
                    self.handle_finalize_completed(input_id, output).await?
                }
                PipelineEvent::Morsel {
                    input_id,
                    partition,
                } => {
                    self.handle_morsel(input_id, partition)?;
                    ControlFlow::Continue
                }
                PipelineEvent::Flush(input_id) => self.handle_flush(input_id).await?,
                PipelineEvent::InputClosed => {
                    self.handle_input_closed()?;
                    ControlFlow::Continue
                }
            };
            if cf == ControlFlow::Stop {
                return Ok(());
            }
        }
        Ok(())
    }
}
