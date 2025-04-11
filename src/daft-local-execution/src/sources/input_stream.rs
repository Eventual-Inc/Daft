use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use common_display::{tree::TreeDisplay, DisplayAs, DisplayLevel};
use common_error::DaftResult;
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_runtime::get_io_runtime;
use daft_core::prelude::SchemaRef;
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_io::IOStatsRef;
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_micropartition::MicroPartition;
use daft_parquet::read::ParquetSchemaInferenceOptions;
use daft_scan::{ChunkSpec, ScanTask, ScanTaskRef};
use daft_warc::WarcConvertOptions;
use futures::{Stream, StreamExt, TryStreamExt};
use snafu::ResultExt;
use tracing::instrument;

use super::scan_task::stream_scan_task;
use crate::{
    channel::Receiver,
    sources::source::{Source, SourceStream},
};

pub struct InputStreamSource {
    rx: Receiver<ScanTaskRef>,
    num_parallel_tasks: usize,
    schema: SchemaRef,
}

impl InputStreamSource {
    const MAX_PARALLEL_SCAN_TASKS: usize = 8;

    pub fn new(rx: Receiver<ScanTaskRef>, schema: SchemaRef) -> Self {
        Self {
            rx,
            num_parallel_tasks: Self::MAX_PARALLEL_SCAN_TASKS,
            schema,
        }
    }

    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }
}

#[async_trait]
impl Source for InputStreamSource {
    #[instrument(name = "InputStreamSource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        maintain_order: bool,
        io_stats: IOStatsRef,
    ) -> DaftResult<SourceStream<'static>> {
        let io_runtime = get_io_runtime(true);
        let rx_stream = futures::stream::unfold(self.rx.clone(), |rx| async move {
            rx.recv().await.map(|scan_task| (scan_task, rx))
        });
        let stream_of_streams = rx_stream.map(move |scan_task| {
            let io_stats = io_stats.clone();
            io_runtime.spawn(stream_scan_task(scan_task, io_stats, None, maintain_order))
        });

        match maintain_order {
            true => {
                let buffered_and_flattened = stream_of_streams
                    .buffered(self.num_parallel_tasks)
                    .map(|r| r?)
                    .try_flatten();
                Ok(Box::pin(buffered_and_flattened))
            }
            false => {
                let buffered_and_flattened = stream_of_streams
                    .then(|r| async { r.await? })
                    .try_flatten_unordered(self.num_parallel_tasks);
                Ok(Box::pin(buffered_and_flattened))
            }
        }
    }

    fn name(&self) -> &'static str {
        "ScanTaskSource"
    }

    fn multiline_display(&self) -> Vec<String> {
        self.display_as(DisplayLevel::Default)
            .lines()
            .map(|s| s.to_string())
            .collect()
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl TreeDisplay for InputStreamSource {
    fn display_as(&self, level: DisplayLevel) -> String {
        fn base_display(_scan: &InputStreamSource) -> String {
            format!("InputStreamSource")
        }
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default | DisplayLevel::Verbose => base_display(self),
        }
    }

    fn get_name(&self) -> String {
        "StreamSource".to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}
