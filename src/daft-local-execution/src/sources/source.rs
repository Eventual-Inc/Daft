use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::stream::BoxStream;

use async_trait::async_trait;

use crate::{channel::MultiSender, pipeline::PipelineNode, ExecutionRuntimeHandle};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

pub trait Source: Send + Sync {
    fn name(&self) -> &'static str;
    fn get_data(
        &self,
        destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> DaftResult<()>;
}

struct SourceNode {
    source: Box<dyn Source>,
}

impl TreeDisplay for SourceNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        self.name().to_string()
    }
    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children().iter().map(|v| v.as_tree_display()).collect()
    }
}

#[async_trait]
impl PipelineNode for SourceNode {
    fn name(&self) -> &'static str {
        self.source.name()
    }
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
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

impl From<Box<dyn Source>> for Box<dyn PipelineNode> {
    fn from(source: Box<dyn Source>) -> Self {
        Box::new(SourceNode { source })
    }
}
