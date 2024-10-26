use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::{
    buffer::RowBasedBuffer, channel::OrderingAwareSender, runtime_stats::CountingReceiver,
};

pub(crate) async fn dispatch(
    receivers: Vec<CountingReceiver>,
    worker_sender: OrderingAwareSender<(usize, Arc<MicroPartition>)>,
    morsel_size: Option<usize>,
) -> DaftResult<()> {
    let mut dispatcher = Dispatcher::new(worker_sender, morsel_size);
    for (idx, receiver) in receivers.into_iter().enumerate() {
        while let Some(morsel) = receiver.recv().await {
            dispatcher.handle_morsel(idx, morsel).await;
        }
        dispatcher.finalize(idx).await;
    }
    Ok(())
}

pub(crate) struct Dispatcher {
    worker_sender: OrderingAwareSender<(usize, Arc<MicroPartition>)>,
    buffer: Option<RowBasedBuffer>,
}

impl Dispatcher {
    fn new(
        worker_sender: OrderingAwareSender<(usize, Arc<MicroPartition>)>,
        morsel_size: Option<usize>,
    ) -> Self {
        let buffer = morsel_size.map(RowBasedBuffer::new);
        Self {
            worker_sender,
            buffer,
        }
    }

    async fn handle_morsel(&mut self, idx: usize, morsel: Arc<MicroPartition>) {
        if let Some(buffer) = self.buffer.as_mut() {
            buffer.push(morsel);
            if let Some(ready) = buffer.pop_enough().unwrap() {
                for r in ready {
                    let _ = self.worker_sender.send((idx, r)).await;
                }
            }
        } else {
            let _ = self.worker_sender.send((idx, morsel)).await;
        }
    }

    async fn finalize(&mut self, idx: usize) {
        if let Some(buffer) = self.buffer.as_mut() {
            // Clear all remaining morsels
            if let Some(last_morsel) = buffer.pop_all().unwrap() {
                let _ = self.worker_sender.send((idx, last_morsel)).await;
            }
        }
    }
}
