use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use tokio::sync::mpsc::error::TrySendError;

use crate::{
    RuntimeHandle, SpawnedTask,
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender, create_channel},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::MorselSizeRequirement,
    plan_input::PipelineMessage,
    runtime_stats::InitializingCountingReceiver,
};

/// The `DispatchSpawner` trait is implemented by types that can spawn a task that reads from
/// an input receiver and distributes morsels to worker receivers.
///
/// The `spawn_dispatch` method is called with the input receiver, the number of workers, the
/// runtime handle, and the pipeline node that the dispatcher is associated with.
///
/// It returns a vector of receivers (one per worker) that will receive the morsels.
///
/// Implementations must spawn a task on the runtime handle that reads from the
/// input receiver and distributes morsels to the worker receivers.
pub(crate) trait DispatchSpawner {
    fn spawn_dispatch(
        &self,
        input_receiver: InitializingCountingReceiver,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult;
}

pub(crate) struct SpawnedDispatchResult {
    pub(crate) worker_receivers: Vec<Receiver<PipelineMessage>>,
    pub(crate) spawned_dispatch_task: SpawnedTask<DaftResult<()>>,
}

/// A dispatcher that distributes morsels to workers in an unordered fashion.
/// Used if the operator does not require maintaining the order of the input.
pub(crate) struct UnorderedDispatcher {
    morsel_size_lower_bound: usize,
    morsel_size_upper_bound: usize,
}

impl UnorderedDispatcher {
    pub(crate) fn new(morsel_size_requirement: MorselSizeRequirement) -> Self {
        let (lower_bound, upper_bound) = match morsel_size_requirement {
            MorselSizeRequirement::Strict(size) => (size, size),
            MorselSizeRequirement::Flexible(lower, upper) => (lower, upper),
        };
        Self {
            morsel_size_lower_bound: lower_bound,
            morsel_size_upper_bound: upper_bound,
        }
    }

    pub(crate) fn unbounded() -> Self {
        Self {
            morsel_size_lower_bound: 0,
            morsel_size_upper_bound: usize::MAX,
        }
    }

    async fn send_to_worker(
        worker_senders: &[Sender<PipelineMessage>],
        mut message: PipelineMessage,
    ) -> Result<(), TrySendError<PipelineMessage>> {
        loop {
            for sender in worker_senders.iter() {
                match sender.try_send(message) {
                    Ok(()) => return Ok(()),
                    Err(TrySendError::Full(message_back)) => {
                        message = message_back;
                        continue;
                    }
                    Err(TrySendError::Closed(message)) => {
                        return Err(TrySendError::Closed(message));
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    }

    async fn dispatch_inner(
        worker_senders: Vec<Sender<PipelineMessage>>,
        mut input_receiver: InitializingCountingReceiver,
        morsel_size_lower_bound: usize,
        morsel_size_upper_bound: usize,
    ) -> DaftResult<()> {
        let mut buffers: HashMap<u32, RowBasedBuffer> = HashMap::new();

        while let Some(msg) = input_receiver.recv().await {
            match &msg {
                PipelineMessage::Morsel {
                    input_id,
                    partition,
                } => {
                    let buffer = buffers.entry(*input_id).or_insert_with(|| {
                        RowBasedBuffer::new(morsel_size_lower_bound, morsel_size_upper_bound)
                    });
                    buffer.push(partition.clone());
                    while let Some(batch) = buffer.next_batch_if_ready()? {
                        let output = PipelineMessage::Morsel {
                            input_id: *input_id,
                            partition: batch,
                        };

                        if Self::send_to_worker(&worker_senders, output).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                PipelineMessage::Flush(input_id) => {
                    // Flush the buffer for this input_id
                    if let Some(buffer) = buffers.get_mut(input_id) {
                        if let Some(last_morsel) = buffer.pop_all()? {
                            let output = PipelineMessage::Morsel {
                                input_id: *input_id,
                                partition: last_morsel,
                            };
                            if Self::send_to_worker(&worker_senders, output).await.is_err() {
                                return Ok(());
                            }
                        }
                        // Remove the buffer after flushing
                        buffers.remove(input_id);
                    }
                    for worker_sender in worker_senders.iter() {
                        let _ = worker_sender.send(PipelineMessage::Flush(*input_id)).await;
                    }
                }
            }
        }

        // Clear all remaining morsels from all buffers at the end
        for (input_id, buffer) in buffers.iter_mut() {
            if let Some(last_morsel) = buffer.pop_all()? {
                let output = PipelineMessage::Morsel {
                    input_id: *input_id,
                    partition: last_morsel,
                };
                let _ = Self::send_to_worker(&worker_senders, output).await;
            }
        }
        Ok(())
    }
}

impl DispatchSpawner for UnorderedDispatcher {
    fn spawn_dispatch(
        &self,
        input_receiver: InitializingCountingReceiver,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult {
        let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
            (0..num_workers).map(|_| create_channel(1)).unzip();
        let morsel_size_lower_bound = self.morsel_size_lower_bound;
        let morsel_size_upper_bound = self.morsel_size_upper_bound;

        let dispatch_task = runtime_handle.spawn(async move {
            Self::dispatch_inner(
                worker_senders,
                input_receiver,
                morsel_size_lower_bound,
                morsel_size_upper_bound,
            )
            .await
        });

        SpawnedDispatchResult {
            worker_receivers,
            spawned_dispatch_task: dispatch_task,
        }
    }
}

/// A dispatcher that distributes morsels to workers in an unordered fashion.
/// Used if the operator does not require maintaining the order of the input.
/// Same as UnorderedDispatcher but can dynamically adjust the batch size based on the batching strategy
pub(crate) struct DynamicUnorderedDispatcher<S: BatchingStrategy + 'static> {
    batch_manager: Arc<BatchManager<S>>,
}

impl<S: BatchingStrategy + 'static> DynamicUnorderedDispatcher<S> {
    pub(crate) fn new(batch_manager: Arc<BatchManager<S>>) -> Self {
        Self { batch_manager }
    }

    async fn dispatch_inner(
        worker_senders: Vec<Sender<PipelineMessage>>,
        mut input_receiver: InitializingCountingReceiver,
        batch_manager: Arc<BatchManager<S>>,
    ) -> DaftResult<()> {
        let (lower, upper) = batch_manager.initial_requirements().values();
        let mut buffers: HashMap<u32, RowBasedBuffer> = HashMap::new();

        while let Some(msg) = input_receiver.recv().await {
            match msg {
                PipelineMessage::Morsel {
                    input_id,
                    partition,
                } => {
                    let buffer = buffers
                        .entry(input_id)
                        .or_insert_with(|| RowBasedBuffer::new(lower, upper));
                    buffer.push(partition);
                    while let Some(batch) = buffer.next_batch_if_ready()? {
                        let output = PipelineMessage::Morsel {
                            input_id,
                            partition: batch,
                        };
                        if UnorderedDispatcher::send_to_worker(&worker_senders, output)
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }

                        let new_requirements = batch_manager.calculate_batch_size();
                        let (lower, upper) = new_requirements.values();

                        buffer.update_bounds(lower, upper);
                    }
                }
                PipelineMessage::Flush(input_id) => {
                    // Flush the buffer for this input_id
                    if let Some(buffer) = buffers.get_mut(&input_id) {
                        if let Some(last_morsel) = buffer.pop_all()? {
                            let output = PipelineMessage::Morsel {
                                input_id,
                                partition: last_morsel,
                            };
                            if UnorderedDispatcher::send_to_worker(&worker_senders, output)
                                .await
                                .is_err()
                            {
                                return Ok(());
                            }
                        }
                        // Remove the buffer after flushing
                        buffers.remove(&input_id);
                    }
                    // Propagate the flush signal - for unbounded, send to all workers
                    for worker_sender in worker_senders.iter() {
                        if worker_sender
                            .send(PipelineMessage::Flush(input_id))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                }
            }
        }

        // Clear all remaining morsels from all buffers at the end
        for (input_id, buffer) in buffers.iter_mut() {
            if let Some(last_morsel) = buffer.pop_all()? {
                let output = PipelineMessage::Morsel {
                    input_id: *input_id,
                    partition: last_morsel,
                };
                if UnorderedDispatcher::send_to_worker(&worker_senders, output)
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

impl<S: BatchingStrategy + 'static> DispatchSpawner for DynamicUnorderedDispatcher<S> {
    fn spawn_dispatch(
        &self,
        input_receiver: InitializingCountingReceiver,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult {
        let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
            (0..num_workers).map(|_| create_channel(1)).unzip();
        let batch_manager = self.batch_manager.clone();

        let dispatch_task = runtime_handle.spawn(async move {
            Self::dispatch_inner(worker_senders, input_receiver, batch_manager).await
        });

        SpawnedDispatchResult {
            worker_receivers,
            spawned_dispatch_task: dispatch_task,
        }
    }
}

/// A dispatcher that distributes morsels to workers based on a partitioning expression.
/// Used if the operator requires partitioning the input, i.e. partitioned writes.
pub(crate) struct PartitionedDispatcher {
    partition_by: Vec<BoundExpr>,
}

impl PartitionedDispatcher {
    pub(crate) fn new(partition_by: Vec<BoundExpr>) -> Self {
        Self { partition_by }
    }

    async fn dispatch_inner(
        worker_senders: Vec<Sender<PipelineMessage>>,
        mut input_receiver: InitializingCountingReceiver,
        partition_by: Vec<BoundExpr>,
    ) -> DaftResult<()> {
        while let Some(msg) = input_receiver.recv().await {
            match msg {
                PipelineMessage::Morsel {
                    input_id,
                    partition,
                } => {
                    let partitions =
                        partition.partition_by_hash(&partition_by, worker_senders.len())?;
                    for (partition, worker_sender) in
                        partitions.into_iter().zip(worker_senders.iter())
                    {
                        let output = PipelineMessage::Morsel {
                            input_id,
                            partition: partition.into(),
                        };
                        if worker_sender.send(output).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                PipelineMessage::Flush(input_id) => {
                    // Propagate flush to all workers
                    for worker_sender in worker_senders.iter() {
                        if worker_sender
                            .send(PipelineMessage::Flush(input_id))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl DispatchSpawner for PartitionedDispatcher {
    fn spawn_dispatch(
        &self,
        input_receiver: InitializingCountingReceiver,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult {
        let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
            (0..num_workers).map(|_| create_channel(1)).unzip();
        let partition_by = self.partition_by.clone();
        let dispatch_task = runtime_handle.spawn(async move {
            Self::dispatch_inner(worker_senders, input_receiver, partition_by).await
        });

        SpawnedDispatchResult {
            worker_receivers,
            spawned_dispatch_task: dispatch_task,
        }
    }
}

/// A dispatcher that distributes morsels to workers based on input_id.
/// Used when operations need to be single-threaded per input_id (e.g., build phase of joins).
pub(crate) struct InputIdDispatcher;

impl InputIdDispatcher {
    pub(crate) fn new() -> Self {
        Self
    }

    async fn dispatch_inner(
        worker_senders: Vec<Sender<PipelineMessage>>,
        mut input_receiver: InitializingCountingReceiver,
    ) -> DaftResult<()> {
        while let Some(msg) = input_receiver.recv().await {
            match msg {
                PipelineMessage::Morsel {
                    input_id,
                    partition,
                } => {
                    // Route to worker based on input_id hash
                    let worker_idx = (input_id as usize) % worker_senders.len();
                    let output = PipelineMessage::Morsel {
                        input_id,
                        partition,
                    };
                    if worker_senders[worker_idx].send(output).await.is_err() {
                        return Ok(());
                    }
                }
                PipelineMessage::Flush(input_id) => {
                    // Route flush to the same worker that handles this input_id
                    let worker_idx = (input_id as usize) % worker_senders.len();
                    if worker_senders[worker_idx]
                        .send(PipelineMessage::Flush(input_id))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }
}

impl DispatchSpawner for InputIdDispatcher {
    fn spawn_dispatch(
        &self,
        input_receiver: InitializingCountingReceiver,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult {
        let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
            (0..num_workers).map(|_| create_channel(1)).unzip();
        let dispatch_task = runtime_handle
            .spawn(async move { Self::dispatch_inner(worker_senders, input_receiver).await });

        SpawnedDispatchResult {
            worker_receivers,
            spawned_dispatch_task: dispatch_task,
        }
    }
}
