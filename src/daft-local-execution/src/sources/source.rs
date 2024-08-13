use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::stream::BoxStream;

use async_trait::async_trait;

use crate::{
    channel::MultiReceiver,
    pipeline::{PipelineNode, PipelineOutputReceiver},
    ExecutionRuntimeHandle,
};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

pub trait Source: Send + Sync {
    fn get_data(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> DaftResult<MultiReceiver>;
}

struct SourceNode {
    source: Box<dyn Source>,
}

#[async_trait]
impl PipelineNode for SourceNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![]
    }
    async fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> DaftResult<PipelineOutputReceiver> {
        let receiver = self.source.get_data(maintain_order, runtime_handle)?;
        Ok(receiver.into())
    }
}

impl From<Box<dyn Source>> for Box<dyn PipelineNode> {
    fn from(source: Box<dyn Source>) -> Self {
        Box::new(SourceNode { source })
    }
}
