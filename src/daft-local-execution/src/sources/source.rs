use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::{stream::BoxStream, StreamExt};

use async_trait::async_trait;
use tracing::Instrument;

use crate::{channel::MultiSender, pipeline::PipelineNode};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

pub trait Source: Send + Sync {
    fn get_data(&self, maintain_order: bool) -> SourceStream;
}

struct SourceNode {
    source_op: Arc<tokio::sync::Mutex<Box<dyn Source>>>,
}

#[async_trait]
impl PipelineNode for SourceNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![]
    }
    async fn start(&mut self, mut destination: MultiSender) -> DaftResult<()> {
        let op = self.source_op.clone();
        tokio::spawn(async move {
            let guard = op.lock().await;
            let mut source_stream = guard.get_data(destination.in_order());
            while let Some(val) = source_stream.next().in_current_span().await {
                let _ = destination.get_next_sender().send(val).await;
            }
            DaftResult::Ok(())
        });
        Ok(())
    }
}

impl From<Box<dyn Source>> for Box<dyn PipelineNode> {
    fn from(value: Box<dyn Source>) -> Self {
        Box::new(SourceNode {
            source_op: Arc::new(tokio::sync::Mutex::new(value)),
        })
    }
}
