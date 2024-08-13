use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::stream::BoxStream;

use async_trait::async_trait;

use crate::{channel::MultiSender, pipeline::PipelineNode, ExecutionRuntimeHandle};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

pub trait Source: Send + Sync {
    fn get_data(
        &self,
        destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> DaftResult<()>;
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
        destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> DaftResult<()> {
        self.source.get_data(destination, runtime_handle)
    }
}

impl From<Box<dyn Source>> for Box<dyn PipelineNode> {
    fn from(source: Box<dyn Source>) -> Self {
        Box::new(SourceNode { source })
    }
}
