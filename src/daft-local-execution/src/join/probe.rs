use std::{
    collections::HashMap,
    ops::ControlFlow,
    sync::Arc,
    time::{Duration, Instant},
};

use common_error::DaftResult;
use common_runtime::JoinSet;
use crate::{
    ExecutionTaskSpawner,
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender, create_channel},
    join::{
        build::BuildStateBridge,
        join_operator::{JoinOperator, ProbeOutput},
    },
    pipeline_message::{InputId, PipelineMessage},
    runtime_stats::RuntimeStats,
};

/// Process all morsels for a single input_id on the probe side, finalize, and send output.
#[allow(clippy::too_many_arguments)]
async fn process_single_input<Op: JoinOperator + 'static>(
    input_id: InputId,
    mut receiver: Receiver<PipelineMessage>,
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    finalize_spawner: ExecutionTaskSpawner,
    runtime_stats: Arc<dyn RuntimeStats>,
    output_sender: Sender<PipelineMessage>,
    build_state_bridge: Arc<BuildStateBridge<Op>>,
    _maintain_order: bool,
) -> DaftResult<ControlFlow<()>> {
    let max_concurrency = op.max_concurrency();
    let mut states: Vec<Op::ProbeState> = (0..max_concurrency)
        .map(|_| {
            let build_rx = build_state_bridge.subscribe(input_id);
            op.make_probe_state(build_rx)
        })
        .collect();

    let (lower, upper) = op
        .morsel_size_requirement()
        .unwrap_or_default()
        .values();
    let mut buffer = RowBasedBuffer::new(lower, upper);

    let mut task_set: JoinSet<DaftResult<(Op::ProbeState, ProbeOutput, Duration)>> =
        JoinSet::new();
    let mut input_closed = false;

    while !input_closed || !task_set.is_empty() {
        // Try to spawn from buffer while states are available
        while !states.is_empty() {
            let batch = buffer.next_batch_if_ready()?;
            if let Some(partition) = batch {
                let state = states.pop().unwrap();
                let op = op.clone();
                let task_spawner = task_spawner.clone();
                task_set.spawn(async move {
                    let now = Instant::now();
                    let (new_state, result) =
                        op.probe(partition, state, &task_spawner).await??;
                    Ok((new_state, result, now.elapsed()))
                });
            } else {
                break;
            }
        }

        if input_closed && task_set.is_empty() {
            break;
        }

        tokio::select! {
            biased;

            Some(result) = task_set.join_next(), if !task_set.is_empty() => {
                let (state, result, elapsed) = result??;
                runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

                // Send output if present
                if let Some(mp) = result.output() {
                    runtime_stats.add_rows_out(mp.len() as u64);
                    if output_sender
                        .send(PipelineMessage::Morsel {
                            input_id,
                            partition: mp.clone(),
                        })
                        .await
                        .is_err()
                    {
                        return Ok(ControlFlow::Break(()));
                    }
                }

                match result {
                    ProbeOutput::NeedMoreInput(_) => {
                        states.push(state);
                    }
                    ProbeOutput::HasMoreOutput { input, .. } => {
                        let op = op.clone();
                        let task_spawner = task_spawner.clone();
                        task_set.spawn(async move {
                            let now = Instant::now();
                            let (new_state, result) =
                                op.probe(input, state, &task_spawner).await??;
                            Ok((new_state, result, now.elapsed()))
                        });
                    }
                }
            }

            msg = receiver.recv(), if !states.is_empty() && !input_closed => {
                match msg {
                    Some(PipelineMessage::Morsel { partition, .. }) => {
                        runtime_stats.add_rows_in(partition.len() as u64);
                        buffer.push(partition);
                    }
                    Some(PipelineMessage::Flush(_)) | None => {
                        input_closed = true;
                    }
                }
            }
        }
    }

    // Drain remaining buffer
    if let Some(mut partition) = buffer.pop_all()? {
        let mut state = states.pop().unwrap();
        loop {
            let now = Instant::now();
            let (new_state, result) =
                op.probe(partition, state, &task_spawner).await??;
            let elapsed = now.elapsed();
            runtime_stats.add_cpu_us(elapsed.as_micros() as u64);

            if let Some(mp) = result.output() {
                runtime_stats.add_rows_out(mp.len() as u64);
                if output_sender
                    .send(PipelineMessage::Morsel {
                        input_id,
                        partition: mp.clone(),
                    })
                    .await
                    .is_err()
                {
                    return Ok(ControlFlow::Break(()));
                }
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

    // Finalize
    if let Some(mp) = op.finalize_probe(states, &finalize_spawner).await?? {
        runtime_stats.add_rows_out(mp.len() as u64);
        if output_sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition: mp,
            })
            .await
            .is_err()
        {
            return Ok(ControlFlow::Break(()));
        }
    }

    // Send flush
    if output_sender
        .send(PipelineMessage::Flush(input_id))
        .await
        .is_err()
    {
        return Ok(ControlFlow::Break(()));
    }
    Ok(ControlFlow::Continue(()))
}

pub(crate) struct ProbeExecutionContext<Op: JoinOperator> {
    op: Arc<Op>,
    task_spawner: ExecutionTaskSpawner,
    finalize_spawner: ExecutionTaskSpawner,
    output_sender: Sender<PipelineMessage>,
    build_state_bridge: Arc<BuildStateBridge<Op>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    maintain_order: bool,
}

impl<Op: JoinOperator + 'static> ProbeExecutionContext<Op> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        op: Arc<Op>,
        task_spawner: ExecutionTaskSpawner,
        finalize_spawner: ExecutionTaskSpawner,
        output_sender: Sender<PipelineMessage>,
        build_state_bridge: Arc<BuildStateBridge<Op>>,
        runtime_stats: Arc<dyn RuntimeStats>,
        maintain_order: bool,
    ) -> Self {
        Self {
            op,
            task_spawner,
            finalize_spawner,
            output_sender,
            build_state_bridge,
            runtime_stats,
            maintain_order,
        }
    }

    pub(crate) async fn process_probe_input(
        &mut self,
        receiver: Receiver<PipelineMessage>,
    ) -> DaftResult<()> {
        let mut receiver = receiver;
        let mut per_input_senders: HashMap<InputId, Sender<PipelineMessage>> = HashMap::new();
        let mut processor_set: JoinSet<DaftResult<ControlFlow<()>>> = JoinSet::new();
        let mut input_closed = false;

        while !input_closed || !processor_set.is_empty() {
            tokio::select! {
                msg = receiver.recv(), if !input_closed => {
                    let Some(msg) = msg else {
                        input_closed = true;
                        per_input_senders.clear();
                        continue;
                    };

                    let input_id = match &msg {
                        PipelineMessage::Morsel { input_id, .. } => *input_id,
                        PipelineMessage::Flush(input_id) => *input_id,
                    };

                    if !per_input_senders.contains_key(&input_id) {
                        let (tx, rx) = create_channel(1);
                        per_input_senders.insert(input_id, tx);

                        let op = self.op.clone();
                        let task_spawner = self.task_spawner.clone();
                        let finalize_spawner = self.finalize_spawner.clone();
                        let runtime_stats = self.runtime_stats.clone();
                        let output_sender = self.output_sender.clone();
                        let build_state_bridge = self.build_state_bridge.clone();
                        let maintain_order = self.maintain_order;
                        processor_set.spawn(async move {
                            process_single_input(
                                input_id, rx, op, task_spawner,
                                finalize_spawner, runtime_stats, output_sender,
                                build_state_bridge, maintain_order,
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
                    if result??.is_break() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
