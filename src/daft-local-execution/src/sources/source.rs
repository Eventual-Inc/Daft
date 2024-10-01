use std::sync::Arc;

use common_display::{tree::TreeDisplay, utils::bytes_to_human_readable};
use daft_io::{IOStatsContext, IOStatsRef};
use daft_micropartition::MicroPartition;
use futures::{stream::BoxStream, StreamExt};

use crate::{
    channel::PipelineChannel, pipeline::PipelineNode, runtime_stats::RuntimeStatsContext,
    ExecutionRuntimeHandle,
};

pub type SourceStream<'a> = BoxStream<'a, Arc<MicroPartition>>;

pub trait Source: Send + Sync {
    fn name(&self) -> &'static str;
    fn get_data(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
        io_stats: IOStatsRef,
    ) -> crate::Result<SourceStream<'static>>;
}

struct SourceNode {
    source: Box<dyn Source>,
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
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<PipelineChannel> {
        let mut source_stream =
            self.source
                .get_data(maintain_order, runtime_handle, self.io_stats.clone())?;

        let mut channel = PipelineChannel::new(1, maintain_order);
        let counting_sender = channel.get_next_sender_with_stats(&self.runtime_stats);
        runtime_handle.spawn(
            async move {
                while let Some(part) = source_stream.next().await {
                    let _ = counting_sender.send(part.into()).await;
                }
                Ok(())
            },
            self.name(),
        );
        Ok(channel)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

impl From<Box<dyn Source>> for Box<dyn PipelineNode> {
    fn from(source: Box<dyn Source>) -> Self {
        let name = source.name();
        Box::new(SourceNode {
            source,
            runtime_stats: RuntimeStatsContext::new(),
            io_stats: IOStatsContext::new(name),
        })
    }
}
