use std::sync::Arc;

use common_display::{tree::TreeDisplay, utils::bytes_to_human_readable};
use common_error::DaftResult;
use daft_io::{IOStatsContext, IOStatsRef};
use daft_micropartition::MicroPartition;
use futures::stream::BoxStream;

use async_trait::async_trait;

use crate::{
    channel::MultiSender, pipeline::PipelineNode, runtime_stats::RuntimeStatsContext,
    ExecutionRuntimeHandle,
};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

pub(crate) trait Source: Send + Sync {
    fn name(&self) -> &'static str;
    fn get_data(
        &self,
        destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
        runtime_stats: Arc<RuntimeStatsContext>,
        io_stats: IOStatsRef,
    ) -> crate::Result<()>;
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
        use common_display::DisplayLevel::*;
        match level {
            Compact => {}
            _ => {
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
    ) -> crate::Result<()> {
        self.source.get_data(
            destination,
            runtime_handle,
            self.runtime_stats.clone(),
            self.io_stats.clone(),
        )
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
