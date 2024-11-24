use std::sync::Arc;

use async_trait::async_trait;
use common_display::{tree::TreeDisplay, utils::bytes_to_human_readable};
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_io::{IOStatsContext, IOStatsRef};
use daft_micropartition::MicroPartition;
use futures::{stream::BoxStream, StreamExt};

use crate::{
    channel::{create_channel, Receiver},
    pipeline::PipelineNode,
    runtime_stats::{CountingSender, RuntimeStatsContext},
    ExecutionRuntimeContext,
};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

#[async_trait]
pub trait Source: Send + Sync {
    fn name(&self) -> &'static str;
    async fn get_data(
        &self,
        maintain_order: bool,
        io_stats: IOStatsRef,
    ) -> DaftResult<SourceStream<'static>>;
    fn schema(&self) -> &SchemaRef;
}

struct SourceNode {
    source: Arc<dyn Source>,
    runtime_stats: Arc<RuntimeStatsContext>,
    io_stats: IOStatsRef,
}

impl TreeDisplay for SourceNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.name()).unwrap();
        use common_display::DisplayLevel::Compact;
        if matches!(level, Compact) {
        } else {
            let rt_result = self.runtime_stats.result();

            writeln!(display).unwrap();
            rt_result.display(&mut display, false, true, false).unwrap();
            let bytes_read = self.io_stats.load_bytes_read();
            writeln!(
                display,
                "bytes read = {}",
                bytes_to_human_readable(bytes_read)
            )
            .unwrap();
        }
        display
    }
    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children()
            .iter()
            .map(|v| v.as_tree_display())
            .collect()
    }
}

impl PipelineNode for SourceNode {
    fn name(&self) -> &'static str {
        self.source.name()
    }
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![]
    }
    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let source = self.source.clone();
        let io_stats = self.io_stats.clone();
        let (destination_sender, destination_receiver) = create_channel(1);
        let counting_sender = CountingSender::new(destination_sender, self.runtime_stats.clone());
        runtime_handle.spawn(
            async move {
                let mut has_data = false;
                let mut source_stream = source.get_data(maintain_order, io_stats).await?;
                while let Some(part) = source_stream.next().await {
                    has_data = true;
                    if counting_sender.send(part?).await.is_err() {
                        return Ok(());
                    }
                }
                if !has_data {
                    let empty = Arc::new(MicroPartition::empty(Some(source.schema().clone())));
                    let _ = counting_sender.send(empty).await;
                }
                Ok(())
            },
            self.name(),
        );
        Ok(destination_receiver)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

impl From<Arc<dyn Source>> for Box<dyn PipelineNode> {
    fn from(source: Arc<dyn Source>) -> Self {
        let name = source.name();
        Box::new(SourceNode {
            source,
            runtime_stats: RuntimeStatsContext::new(),
            io_stats: IOStatsContext::new(name),
        })
    }
}
