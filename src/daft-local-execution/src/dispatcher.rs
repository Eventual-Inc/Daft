use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;

use crate::{
    buffer::RowBasedBuffer,
    channel::{create_channel, Receiver, Sender},
    runtime_stats::CountingReceiver,
    RuntimeHandle, SpawnedTask,
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
        input_receivers: Vec<CountingReceiver>,
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
pub(crate) struct RoundRobinDispatcher {
    morsel_size_lower_bound: usize,
    morsel_size_upper_bound: usize,
}

impl RoundRobinDispatcher {
    pub(crate) fn new(morsel_size_lower_bound: usize, morsel_size_upper_bound: usize) -> Self {
        Self {
            morsel_size_lower_bound,
            morsel_size_upper_bound,
        }
    }

    pub(crate) fn with_fixed_threshold(threshold: usize) -> Self {
        Self {
            morsel_size_lower_bound: threshold,
            morsel_size_upper_bound: threshold,
        }
    }

    pub(crate) fn unbounded() -> Self {
        Self {
            morsel_size_lower_bound: 0,
            morsel_size_upper_bound: usize::MAX,
        }
    }

    async fn dispatch_inner(
        worker_senders: Vec<Sender<Arc<MicroPartition>>>,
        input_receivers: Vec<CountingReceiver>,
        morsel_size_lower_bound: usize,
        morsel_size_upper_bound: usize,
    ) -> DaftResult<()> {
        let mut next_worker_idx = 0;
        let mut send_to_next_worker = |data: Arc<MicroPartition>| {
            let next_worker_sender = worker_senders.get(next_worker_idx).unwrap();
            next_worker_idx = (next_worker_idx + 1) % worker_senders.len();
            next_worker_sender.send(data)
        };

        for receiver in input_receivers {
            let mut buffer = RowBasedBuffer::new(morsel_size_lower_bound, morsel_size_upper_bound);

            while let Some(morsel) = receiver.recv().await {
                buffer.push(&morsel);
                if let Some(ready) = buffer.pop_enough()? {
                    for r in ready {
                        if send_to_next_worker(r).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }

            // Clear all remaining morsels
            if let Some(last_morsel) = buffer.pop_all()? {
                if send_to_next_worker(last_morsel).await.is_err() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

impl DispatchSpawner for RoundRobinDispatcher {
    fn spawn_dispatch(
        &self,
        input_receivers: Vec<CountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult {
        let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
            (0..num_workers).map(|_| create_channel(0)).unzip();
        let morsel_size_lower_bound = self.morsel_size_lower_bound;
        let morsel_size_upper_bound = self.morsel_size_upper_bound;
        let task = runtime_handle.spawn(async move {
            Self::dispatch_inner(
                worker_senders,
                input_receivers,
                morsel_size_lower_bound,
                morsel_size_upper_bound,
            )
            .await
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
    pub(crate) fn new(morsel_size_lower_bound: usize, morsel_size_upper_bound: usize) -> Self {
        Self {
            morsel_size_lower_bound,
            morsel_size_upper_bound,
        }
    }

    pub(crate) fn with_fixed_threshold(threshold: usize) -> Self {
        Self {
            morsel_size_lower_bound: threshold,
            morsel_size_upper_bound: threshold,
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
        input_receivers: Vec<CountingReceiver>,
        morsel_size_lower_bound: usize,
        morsel_size_upper_bound: usize,
    ) -> DaftResult<()> {
        for receiver in input_receivers {
            let mut buffer = RowBasedBuffer::new(morsel_size_lower_bound, morsel_size_upper_bound);

            while let Some(morsel) = receiver.recv().await {
                buffer.push(&morsel);
                if let Some(ready) = buffer.pop_enough()? {
                    for r in ready {
                        if worker_sender.send(r).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }

            // Clear all remaining morsels
            if let Some(last_morsel) = buffer.pop_all()? {
                if worker_sender.send(last_morsel).await.is_err() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

impl DispatchSpawner for UnorderedDispatcher {
    fn spawn_dispatch(
        &self,
        receiver: Vec<CountingReceiver>,
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
        input_receivers: Vec<CountingReceiver>,
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
        input_receivers: Vec<CountingReceiver>,
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
