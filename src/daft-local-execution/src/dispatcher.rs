use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_dsl::ExprRef;

use crate::{
    buffer::RowBasedBuffer, channel::Sender, pipeline::PipelineResultType,
    runtime_stats::CountingReceiver,
};

#[async_trait]
pub(crate) trait Dispatcher {
    async fn dispatch(
        &self,
        receivers: Vec<CountingReceiver>,
        worker_senders: Vec<Sender<(usize, PipelineResultType)>>,
    ) -> DaftResult<()>;
}

pub(crate) struct RoundRobinBufferedDispatcher {
    morsel_size: Option<usize>,
}

impl RoundRobinBufferedDispatcher {
    pub(crate) fn new(morsel_size: Option<usize>) -> Self {
        Self { morsel_size }
    }
}

#[async_trait]
impl Dispatcher for RoundRobinBufferedDispatcher {
    async fn dispatch(
        &self,
        receivers: Vec<CountingReceiver>,
        worker_senders: Vec<Sender<(usize, PipelineResultType)>>,
    ) -> DaftResult<()> {
        let mut next_worker_idx = 0;
        let mut send_to_next_worker = |idx, data: PipelineResultType| {
            let next_worker_sender = worker_senders.get(next_worker_idx).unwrap();
            next_worker_idx = (next_worker_idx + 1) % worker_senders.len();
            next_worker_sender.send((idx, data))
        };

        let mut buffer = self.morsel_size.map(RowBasedBuffer::new);
        for (idx, mut receiver) in receivers.into_iter().enumerate() {
            while let Some(morsel) = receiver.recv().await {
                if morsel.should_broadcast() {
                    for worker_sender in &worker_senders {
                        if worker_sender.send((idx, morsel.clone())).await.is_err() {
                            return Ok(());
                        }
                    }
                } else if let Some(ref mut buffer) = buffer {
                    buffer.push(morsel.as_data());
                    if let Some(ready) = buffer.pop_enough()? {
                        for r in ready {
                            if send_to_next_worker(idx, r.into()).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                } else if send_to_next_worker(idx, morsel).await.is_err() {
                    return Ok(());
                }
            }
            // Clear all remaining morsels
            if let Some(ref mut buffer) = buffer {
                if let Some(last_morsel) = buffer.pop_all()? {
                    let _ = send_to_next_worker(idx, last_morsel.into()).await;
                }
            }
        }
        Ok(())
    }
}

pub(crate) struct PartitionedDispatcher {
    partition_by: Vec<ExprRef>,
}

impl PartitionedDispatcher {
    pub(crate) fn new(partition_by: Vec<ExprRef>) -> Self {
        Self { partition_by }
    }
}

#[async_trait]
impl Dispatcher for PartitionedDispatcher {
    async fn dispatch(
        &self,
        receivers: Vec<CountingReceiver>,
        worker_senders: Vec<Sender<(usize, PipelineResultType)>>,
    ) -> DaftResult<()> {
        for (idx, mut receiver) in receivers.into_iter().enumerate() {
            while let Some(morsel) = receiver.recv().await {
                if morsel.should_broadcast() {
                    for worker_sender in &worker_senders {
                        if worker_sender.send((idx, morsel.clone())).await.is_err() {
                            return Ok(());
                        }
                    }
                } else {
                    let partitions = morsel
                        .as_data()
                        .partition_by_hash(&self.partition_by, worker_senders.len())?;
                    for (partition, worker_sender) in
                        partitions.into_iter().zip(worker_senders.iter())
                    {
                        if worker_sender
                            .send((idx, Arc::new(partition).into()))
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
