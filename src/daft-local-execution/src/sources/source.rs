use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::{stream::BoxStream, StreamExt};
use tracing::{instrument, Instrument};

use crate::{channel::MultiSender, TaskSet};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

pub trait Source: Send + Sync {
    fn get_data(&self) -> SourceStream;
}

pub struct SourceRunner {
    source: Arc<dyn Source>,
    sender: MultiSender,
}

impl SourceRunner {
    pub fn new(source: Arc<dyn Source>, sender: MultiSender) -> Self {
        Self { source, sender }
    }

    #[instrument(level = "info", skip(self), name = "SourceActor::run")]
    pub async fn run(&mut self) -> DaftResult<()> {
        let mut source_stream = self.source.get_data();
        while let Some(val) = source_stream.next().in_current_span().await {
            let _ = self.sender.get_next_sender().send(val?).await;
        }
        Ok(())
    }
}

pub fn run_source(source: Arc<dyn Source>, sender: MultiSender, op_set: &mut TaskSet<()>) {
    let mut runner = SourceRunner::new(source, sender);
    op_set.spawn(async move { runner.run().await });
}
