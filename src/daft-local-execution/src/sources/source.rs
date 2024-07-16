use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::{stream::BoxStream, StreamExt};

use crate::MultiSender;

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

pub trait Source: Send + Sync {
    fn get_data(&self) -> SourceStream;
}

pub struct SourceActor {
    source: Arc<dyn Source>,
    sender: MultiSender,
}

impl SourceActor {
    pub fn new(source: Arc<dyn Source>, sender: MultiSender) -> Self {
        Self { source, sender }
    }

    pub async fn run(&mut self) -> DaftResult<()> {
        let mut source_stream = self.source.get_data();
        while let Some(val) = source_stream.next().await {
            let _ = self.sender.get_next_sender().send(val).await;
        }
        Ok(())
    }
}

pub fn run_source(source: Arc<dyn Source>, sender: MultiSender) {
    let mut actor = SourceActor::new(source, sender);
    tokio::spawn(async move {
        let _ = actor.run().await;
    });
}
