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

pub(crate) enum Dispatcher {
    RoundRobin { morsel_size: Option<usize> },
    Unordered { morsel_size: Option<usize> },
    Partitioned { partition_by: Vec<ExprRef> },
}

impl Dispatcher {
    pub(crate) fn spawn_dispatch_task(
        &self,
        input_receivers: Vec<CountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut ExecutionRuntimeHandle,
        name: &'static str,
    ) -> Vec<Receiver<Arc<MicroPartition>>> {
        match self {
            Self::RoundRobin { morsel_size } => {
                let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
                    (0..num_workers).map(|_| create_channel(1)).unzip();
                runtime_handle.spawn(
                    Self::dispatch_round_robin(worker_senders, input_receivers, *morsel_size),
                    name,
                );
                worker_receivers
            }
            Self::Unordered { morsel_size } => {
                let (worker_sender, worker_receiver) = create_channel(num_workers);
                let worker_receivers = vec![worker_receiver; num_workers];
                runtime_handle.spawn(
                    Self::dispatch_unordered(worker_sender, input_receivers, *morsel_size),
                    name,
                );
                worker_receivers
            }
            Self::Partitioned { partition_by } => {
                let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
                    (0..num_workers).map(|_| create_channel(1)).unzip();
                runtime_handle.spawn(
                    Self::dispatch_partitioned(
                        worker_senders,
                        input_receivers,
                        partition_by.clone(),
                    ),
                    name,
                );
                worker_receivers
            }
        }
    }

    async fn dispatch_round_robin(
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

    async fn dispatch_unordered(
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

    async fn dispatch_partitioned(
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
