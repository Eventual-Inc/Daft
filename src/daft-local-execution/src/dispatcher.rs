use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;

use crate::{
    RuntimeHandle, SpawnedTask,
    buffer::RowBasedBuffer,
    channel::{Receiver, Sender, create_channel},
    dynamic_batching::{BatchManager, BatchingStrategy},
    pipeline::MorselSizeRequirement,
    runtime_stats::InitializingCountingReceiver,
};

/// The `DispatchSpawner` trait is implemented by types that can spawn a task that reads from
/// input receivers and distributes morsels to worker receivers.
///
/// The `spawn_dispatch` method is called with the input receivers, the number of workers, the
/// runtime handle, and the pipeline node that the dispatcher is associated with.
///
/// It returns a vector of receivers (one per worker) that will receive the morsels.
///
/// Implementations must spawn a task on the runtime handle that reads from the
/// input receivers and distributes morsels to the worker receivers.
pub(crate) trait DispatchSpawner {
    fn spawn_dispatch(
        &self,
        input_receivers: Vec<InitializingCountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult;
}

pub(crate) struct SpawnedDispatchResult {
    pub(crate) worker_receivers: Vec<Receiver<Arc<MicroPartition>>>,
    pub(crate) spawned_dispatch_task: SpawnedTask<DaftResult<()>>,
}

/// A dispatcher that distributes morsels to workers in a round-robin fashion.
/// Used if the operator requires maintaining the order of the input.
pub(crate) struct RoundRobinDispatcher<S: BatchingStrategy + 'static> {
    batch_manager: Arc<BatchManager<S>>,
}

impl<S: BatchingStrategy + 'static> RoundRobinDispatcher<S> {
    pub(crate) fn new(batch_manager: Arc<BatchManager<S>>) -> Self {
        Self { batch_manager }
    }

    async fn dispatch_inner(
        worker_senders: Vec<Sender<Arc<MicroPartition>>>,
        input_receivers: Vec<InitializingCountingReceiver>,
        batch_manager: Arc<BatchManager<S>>,
    ) -> DaftResult<()> {
        let mut next_worker_idx = 0;
        let mut send_to_next_worker = |data: Arc<MicroPartition>| {
            let next_worker_sender = worker_senders.get(next_worker_idx).unwrap();
            next_worker_idx = (next_worker_idx + 1) % worker_senders.len();
            next_worker_sender.send(data)
        };

        let (lower, upper) = batch_manager.initial_requirements().values();
        for receiver in input_receivers {
            let mut buffer = RowBasedBuffer::new(lower, upper);

            while let Some(morsel) = receiver.recv().await {
                buffer.push(&morsel);

                while let Some(batch) = buffer.next_batch_if_ready()? {
                    if send_to_next_worker(batch).await.is_err() {
                        return Ok(());
                    }
                    let new_requirements = batch_manager.calculate_batch_size();
                    let (lower, upper) = new_requirements.values();

                    buffer.update_bounds(lower, upper);
                }
            }

            // Clear all remaining morsels
            if let Some(last_morsel) = buffer.pop_all()?
                && send_to_next_worker(last_morsel).await.is_err()
            {
                return Ok(());
            }
        }
        Ok(())
    }
}

impl<S: BatchingStrategy + 'static> DispatchSpawner for RoundRobinDispatcher<S> {
    fn spawn_dispatch(
        &self,
        input_receivers: Vec<InitializingCountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult {
        let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
            (0..num_workers).map(|_| create_channel(0)).unzip();
        let batch_manager = self.batch_manager.clone();
        let task = runtime_handle.spawn(async move {
            Self::dispatch_inner(worker_senders, input_receivers, batch_manager).await
        });

        SpawnedDispatchResult {
            worker_receivers,
            spawned_dispatch_task: task,
        }
    }
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

    async fn dispatch_inner(
        worker_sender: Sender<Arc<MicroPartition>>,
        input_receivers: Vec<InitializingCountingReceiver>,
        morsel_size_lower_bound: usize,
        morsel_size_upper_bound: usize,
    ) -> DaftResult<()> {
        for receiver in input_receivers {
            let mut buffer = RowBasedBuffer::new(morsel_size_lower_bound, morsel_size_upper_bound);

            while let Some(morsel) = receiver.recv().await {
                buffer.push(&morsel);
                while let Some(batch) = buffer.next_batch_if_ready()? {
                    if worker_sender.send(batch).await.is_err() {
                        return Ok(());
                    }
                }
            }

            // Clear all remaining morsels
            if let Some(last_morsel) = buffer.pop_all()?
                && worker_sender.send(last_morsel).await.is_err()
            {
                return Ok(());
            }
        }
        Ok(())
    }
}

impl DispatchSpawner for UnorderedDispatcher {
    fn spawn_dispatch(
        &self,
        receiver: Vec<InitializingCountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult {
        let (worker_sender, worker_receiver) = create_channel(num_workers);
        let worker_receivers = vec![worker_receiver; num_workers];
        let morsel_size_lower_bound = self.morsel_size_lower_bound;
        let morsel_size_upper_bound = self.morsel_size_upper_bound;

        let dispatch_task = runtime_handle.spawn(async move {
            Self::dispatch_inner(
                worker_sender,
                receiver,
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
        worker_sender: Sender<Arc<MicroPartition>>,
        input_receivers: Vec<InitializingCountingReceiver>,
        batch_manager: Arc<BatchManager<S>>,
    ) -> DaftResult<()> {
        let (lower, upper) = batch_manager.initial_requirements().values();

        for receiver in input_receivers {
            let mut buffer = RowBasedBuffer::new(lower, upper);

            while let Some(morsel) = receiver.recv().await {
                buffer.push(&morsel);
                while let Some(batch) = buffer.next_batch_if_ready()? {
                    if worker_sender.send(batch).await.is_err() {
                        return Ok(());
                    }

                    let new_requirements = batch_manager.calculate_batch_size();
                    let (lower, upper) = new_requirements.values();

                    buffer.update_bounds(lower, upper);
                }
            }

            // Clear all remaining morsels
            if let Some(last_morsel) = buffer.pop_all()?
                && worker_sender.send(last_morsel).await.is_err()
            {
                return Ok(());
            }
        }
        Ok(())
    }
}

impl<S: BatchingStrategy + 'static> DispatchSpawner for DynamicUnorderedDispatcher<S> {
    fn spawn_dispatch(
        &self,
        receiver: Vec<InitializingCountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult {
        let (worker_sender, worker_receiver) = create_channel(num_workers);
        let worker_receivers = vec![worker_receiver; num_workers];
        let batch_manager = self.batch_manager.clone();

        let dispatch_task = runtime_handle.spawn(async move {
            Self::dispatch_inner(worker_sender, receiver, batch_manager).await
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
        worker_senders: Vec<Sender<Arc<MicroPartition>>>,
        input_receivers: Vec<InitializingCountingReceiver>,
        partition_by: Vec<BoundExpr>,
    ) -> DaftResult<()> {
        for receiver in input_receivers {
            while let Some(morsel) = receiver.recv().await {
                let partitions = morsel.partition_by_hash(&partition_by, worker_senders.len())?;
                for (partition, worker_sender) in partitions.into_iter().zip(worker_senders.iter())
                {
                    if worker_sender.send(Arc::new(partition)).await.is_err() {
                        return Ok(());
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
        input_receivers: Vec<InitializingCountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult {
        let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
            (0..num_workers).map(|_| create_channel(0)).unzip();
        let partition_by = self.partition_by.clone();
        let dispatch_task = runtime_handle.spawn(async move {
            Self::dispatch_inner(worker_senders, input_receivers, partition_by).await
        });

        SpawnedDispatchResult {
            worker_receivers,
            spawned_dispatch_task: dispatch_task,
        }
    }
}
