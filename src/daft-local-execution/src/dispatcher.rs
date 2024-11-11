use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

use crate::{
    buffer::RowBasedBuffer,
    channel::{create_channel, Receiver, Sender},
    runtime_stats::CountingReceiver,
    ExecutionRuntimeHandle,
};

/// The `Dispatcher` trait defines the interface for dispatching morsels to workers.
///
/// The `dispatch` method takes:
/// - A vector of input receivers
/// - The number of workers
/// - A runtime handle for the current execution context
/// - The name of the operator
///
/// It returns a vector of receivers (one per worker) that will receive the morsels.
///
/// Implementations must spawn a task on the runtime handle that reads from the
/// input receivers and distributes morsels to the worker receivers.
pub(crate) trait Dispatcher {
    fn dispatch(
        &self,
        input_receivers: Vec<CountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut ExecutionRuntimeHandle,
        name: &'static str,
    ) -> Vec<Receiver<Arc<MicroPartition>>>;
}

/// A dispatcher that distributes morsels to workers in a round-robin fashion.
/// Used if the operator requires maintaining the order of the input.
pub(crate) struct RoundRobinDispatcher {
    morsel_size: Option<usize>,
}

impl RoundRobinDispatcher {
    pub(crate) fn new(morsel_size: Option<usize>) -> Self {
        Self { morsel_size }
    }

    async fn dispatch_inner(
        worker_senders: Vec<Sender<Arc<MicroPartition>>>,
        input_receivers: Vec<CountingReceiver>,
        morsel_size: Option<usize>,
    ) -> DaftResult<()> {
        let mut next_worker_idx = 0;
        let mut send_to_next_worker = |data: Arc<MicroPartition>| {
            let next_worker_sender = worker_senders.get(next_worker_idx).unwrap();
            next_worker_idx = (next_worker_idx + 1) % worker_senders.len();
            next_worker_sender.send(data)
        };

        for receiver in input_receivers {
            let mut buffer = morsel_size.map(RowBasedBuffer::new);
            while let Some(morsel) = receiver.recv().await {
                if let Some(buffer) = &mut buffer {
                    buffer.push(&morsel);
                    if let Some(ready) = buffer.pop_enough()? {
                        for r in ready {
                            if send_to_next_worker(r).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                } else if send_to_next_worker(morsel).await.is_err() {
                    return Ok(());
                }
            }
            // Clear all remaining morsels
            if let Some(buffer) = &mut buffer {
                if let Some(last_morsel) = buffer.pop_all()? {
                    if send_to_next_worker(last_morsel).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }
}

impl Dispatcher for RoundRobinDispatcher {
    fn dispatch(
        &self,
        input_receivers: Vec<CountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut ExecutionRuntimeHandle,
        name: &'static str,
    ) -> Vec<Receiver<Arc<MicroPartition>>> {
        let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
            (0..num_workers).map(|_| create_channel(1)).unzip();
        let morsel_size = self.morsel_size;
        runtime_handle.spawn(
            Self::dispatch_inner(worker_senders, input_receivers, morsel_size),
            name,
        );
        worker_receivers
    }
}

/// A dispatcher that distributes morsels to workers in an unordered fashion.
/// Used if the operator does not require maintaining the order of the input.
pub(crate) struct UnorderedDispatcher {
    morsel_size: Option<usize>,
}

impl UnorderedDispatcher {
    pub(crate) fn new(morsel_size: Option<usize>) -> Self {
        Self { morsel_size }
    }

    async fn dispatch_inner(
        worker_sender: Sender<Arc<MicroPartition>>,
        input_receivers: Vec<CountingReceiver>,
        morsel_size: Option<usize>,
    ) -> DaftResult<()> {
        for receiver in input_receivers {
            let mut buffer = morsel_size.map(RowBasedBuffer::new);
            while let Some(morsel) = receiver.recv().await {
                if let Some(buffer) = &mut buffer {
                    buffer.push(&morsel);
                    if let Some(ready) = buffer.pop_enough()? {
                        for r in ready {
                            if worker_sender.send(r).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                } else if worker_sender.send(morsel).await.is_err() {
                    return Ok(());
                }
            }
            // Clear all remaining morsels
            if let Some(buffer) = &mut buffer {
                if let Some(last_morsel) = buffer.pop_all()? {
                    if worker_sender.send(last_morsel).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }
}

impl Dispatcher for UnorderedDispatcher {
    fn dispatch(
        &self,
        receiver: Vec<CountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut ExecutionRuntimeHandle,
        name: &'static str,
    ) -> Vec<Receiver<Arc<MicroPartition>>> {
        let (worker_sender, worker_receiver) = create_channel(num_workers);
        let worker_receivers = vec![worker_receiver; num_workers];
        let morsel_size = self.morsel_size;
        runtime_handle.spawn(
            Self::dispatch_inner(worker_sender, receiver, morsel_size),
            name,
        );
        worker_receivers
    }
}

/// A dispatcher that distributes morsels to workers based on a partitioning expression.
/// Used if the operator requires partitioning the input, i.e. partitioned writes.
pub(crate) struct PartitionedDispatcher {
    partition_by: Vec<ExprRef>,
}

impl PartitionedDispatcher {
    pub(crate) fn new(partition_by: Vec<ExprRef>) -> Self {
        Self { partition_by }
    }

    async fn dispatch_inner(
        worker_senders: Vec<Sender<Arc<MicroPartition>>>,
        input_receivers: Vec<CountingReceiver>,
        partition_by: Vec<ExprRef>,
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

impl Dispatcher for PartitionedDispatcher {
    fn dispatch(
        &self,
        input_receivers: Vec<CountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut ExecutionRuntimeHandle,
        name: &'static str,
    ) -> Vec<Receiver<Arc<MicroPartition>>> {
        let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
            (0..num_workers).map(|_| create_channel(1)).unzip();
        let partition_by = self.partition_by.clone();
        runtime_handle.spawn(
            Self::dispatch_inner(worker_senders, input_receivers, partition_by),
            name,
        );
        worker_receivers
    }
}
