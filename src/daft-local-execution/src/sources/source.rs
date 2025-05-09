use std::sync::Arc;

use async_trait::async_trait;
use common_display::{tree::TreeDisplay, utils::bytes_to_human_readable};
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_io::{IOStatsContext, IOStatsRef};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use futures::{stream::BoxStream, StreamExt};

use crate::{
    channel::{create_channel, Receiver},
    pipeline::PipelineNode,
    progress_bar::ProgressBarColor,
    runtime_stats::{CountingSender, RuntimeStatsContext},
    ExecutionRuntimeContext,
};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

#[async_trait]
pub trait Source: Send + Sync {
    fn name(&self) -> &'static str;
    fn multiline_display(&self) -> Vec<String>;
    async fn get_data(
        &self,
        maintain_order: bool,
        io_stats: IOStatsRef,
    ) -> DaftResult<SourceStream<'static>>;
    fn schema(&self) -> &SchemaRef;
}

pub(crate) struct SourceNode {
    source: Arc<dyn Source>,
    runtime_stats: Arc<RuntimeStatsContext>,
    plan_stats: StatsState,
    io_stats: IOStatsRef,
}

impl SourceNode {
    pub fn new(source: Arc<dyn Source>, plan_stats: StatsState) -> Self {
        let runtime_stats = RuntimeStatsContext::new(source.name());
        let io_stats = IOStatsContext::new(source.name());
        Self {
            source,
            runtime_stats,
            plan_stats,
            io_stats,
        }
    }

    pub fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

impl TreeDisplay for SourceNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        use common_display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.source.name()).unwrap();
            }
            level => {
                let multiline_display = self.source.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();

                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }

                if matches!(level, DisplayLevel::Verbose) {
                    let rt_result = self.runtime_stats.result();

                    writeln!(display).unwrap();
                    rt_result.display(&mut display, false, true, false).unwrap();
                    let bytes_read = self.io_stats.load_bytes_read();
                    writeln!(
                        display,
                        "Bytes read = {}",
                        bytes_to_human_readable(bytes_read)
                    )
                    .unwrap();
                }
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
        let progress_bar = runtime_handle.make_progress_bar(
            self.name(),
            ProgressBarColor::Blue,
            false,
            self.runtime_stats.clone(),
        );
        let source = self.source.clone();
        let io_stats = self.io_stats.clone();
        let (destination_sender, destination_receiver) = create_channel(0);
        let counting_sender =
            CountingSender::new(destination_sender, self.runtime_stats.clone(), progress_bar);
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
