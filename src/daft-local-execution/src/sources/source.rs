use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::{stream::BoxStream, StreamExt};

use async_trait::async_trait;
use tracing::Instrument;

use crate::{channel::MultiSender, pipeline::PipelineNode, WORKER_SET};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

pub trait Source: Send + Sync {
    fn get_data(&self, maintain_order: bool) -> SourceStream;
}

struct SourceNode {
    source_ops: Vec<Arc<tokio::sync::Mutex<Box<dyn Source>>>>,
}

#[async_trait]
impl PipelineNode for SourceNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![]
    }
    async fn start(&mut self, mut destination: MultiSender) -> DaftResult<()> {
        let maintain_order = destination.in_order();
        for source_op in &self.source_ops {
            let op = source_op.clone();
            let sender = destination.get_next_sender();
            WORKER_SET.lock().await.spawn(async move {
                let guard = op.lock().await;
                let mut source_stream = guard.get_data(maintain_order);
                while let Some(val) = source_stream.next().in_current_span().await {
                    let _ = sender.send(val?).await;
                }
                DaftResult::Ok(())
            });
        }
        Ok(())
    }
}

impl From<Vec<Box<dyn Source>>> for Box<dyn PipelineNode> {
    fn from(sources: Vec<Box<dyn Source>>) -> Self {
        Box::new(SourceNode {
            source_ops: sources
                .into_iter()
                .map(|v| Arc::new(tokio::sync::Mutex::new(v)))
                .collect(),
        })
    }
}
