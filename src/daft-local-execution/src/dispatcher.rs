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
        receiver: CountingReceiver,
        worker_senders: Vec<Sender<PipelineResultType>>,
    ) -> DaftResult<()>;
}

pub(crate) struct RoundRobinBufferedDispatcher {
    morsel_size: usize,
}

impl RoundRobinBufferedDispatcher {
    pub(crate) fn new(morsel_size: usize) -> Self {
        Self { morsel_size }
    }
}

#[async_trait]
impl Dispatcher for RoundRobinBufferedDispatcher {
    async fn dispatch(
        &self,
        mut receiver: CountingReceiver,
        worker_senders: Vec<Sender<PipelineResultType>>,
    ) -> DaftResult<()> {
        let mut next_worker_idx = 0;
        let mut send_to_next_worker = |data: PipelineResultType| {
            let next_worker_sender = worker_senders.get(next_worker_idx).unwrap();
            next_worker_idx = (next_worker_idx + 1) % worker_senders.len();
            next_worker_sender.send(data)
        };

        let mut buffer = RowBasedBuffer::new(self.morsel_size);
        while let Some(morsel) = receiver.recv().await {
            if morsel.should_broadcast() {
                for worker_sender in &worker_senders {
                    let _ = worker_sender.send(morsel.clone()).await;
                }
            } else {
                buffer.push(morsel.as_data());
                if let Some(ready) = buffer.pop_enough()? {
                    for r in ready {
                        let _ = send_to_next_worker(r.into()).await;
                    }
                }
            }
        }
        // Clear all remaining morsels
        if let Some(last_morsel) = buffer.pop_all()? {
            let _ = send_to_next_worker(last_morsel.into()).await;
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
        mut receiver: CountingReceiver,
        worker_senders: Vec<Sender<PipelineResultType>>,
    ) -> DaftResult<()> {
        while let Some(morsel) = receiver.recv().await {
            if morsel.should_broadcast() {
                for worker_sender in &worker_senders {
                    let _ = worker_sender.send(morsel.clone()).await;
                }
            } else {
                let partitions = morsel
                    .as_data()
                    .partition_by_hash(&self.partition_by, worker_senders.len())?;
                for (partition, worker_sender) in partitions.into_iter().zip(worker_senders.iter())
                {
                    let _ = worker_sender.send(Arc::new(partition).into()).await;
                }
            }
        }
        Ok(())
    }
}
