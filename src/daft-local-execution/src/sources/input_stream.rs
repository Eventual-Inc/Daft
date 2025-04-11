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
            io_runtime.spawn(stream_scan_task(scan_task, io_stats, maintain_order))
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
        "ScanTaskSource".to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}

async fn stream_scan_task(
    scan_task: Arc<ScanTask>,
    io_stats: IOStatsRef,
    maintain_order: bool,
) -> DaftResult<impl Stream<Item = DaftResult<Arc<MicroPartition>>> + Send> {
    let pushdown_columns = scan_task.pushdowns.columns.as_ref().map(|v| {
        v.iter()
            .map(std::string::String::as_str)
            .collect::<Vec<&str>>()
    });

    let file_column_names = match (
        pushdown_columns,
        scan_task.partition_spec().map(|ps| ps.to_fill_map()),
    ) {
        (None, _) => None,
        (Some(columns), None) => Some(columns.clone()),

        // If the ScanTask has a partition_spec, we elide reads of partition columns from the file
        (Some(columns), Some(partition_fillmap)) => Some(
            columns
                .iter()
                .filter_map(|s| {
                    if partition_fillmap.contains_key(s) {
                        None
                    } else {
                        Some(*s)
                    }
                })
                .collect::<Vec<&str>>(),
        ),
    };

    if scan_task.sources.len() != 1 {
        return Err(common_error::DaftError::TypeError(
            "Streaming reads only supported for single source ScanTasks".to_string(),
        ));
    }
    let source = scan_task.sources.first().unwrap();
    let url = source.get_path();
    let io_config = Arc::new(
        scan_task
            .storage_config
            .io_config
            .clone()
            .unwrap_or_default(),
    );
    let io_client = daft_io::get_io_client(scan_task.storage_config.multithreaded_io, io_config)?;
    let table_stream = match scan_task.file_format_config.as_ref() {
        FileFormatConfig::Parquet(ParquetSourceConfig {
            coerce_int96_timestamp_unit,
            field_id_mapping,
            chunk_size,
            ..
        }) => {
            let inference_options =
                ParquetSchemaInferenceOptions::new(Some(*coerce_int96_timestamp_unit));

            let row_groups = if let Some(ChunkSpec::Parquet(row_groups)) = source.get_chunk_spec() {
                Some(row_groups.clone())
            } else {
                None
            };
            let metadata = scan_task
                .sources
                .first()
                .and_then(|s| s.get_parquet_metadata().cloned());
            daft_parquet::read::stream_parquet(
                url,
                file_column_names.as_deref(),
                scan_task.pushdowns.limit,
                row_groups,
                scan_task.pushdowns.filters.clone(),
                io_client,
                Some(io_stats),
                &inference_options,
                field_id_mapping.clone(),
                metadata,
                maintain_order,
                None,
                *chunk_size,
            )
            .await?
        }
        FileFormatConfig::Csv(cfg) => {
            let schema_of_file = scan_task.schema.clone();
            let col_names = if !cfg.has_headers {
                Some(
                    schema_of_file
                        .fields
                        .values()
                        .map(|f| f.name.as_str())
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            };
            let convert_options = CsvConvertOptions::new_internal(
                scan_task.pushdowns.limit,
                file_column_names
                    .as_ref()
                    .map(|cols| cols.iter().map(|col| (*col).to_string()).collect()),
                col_names
                    .as_ref()
                    .map(|cols| cols.iter().map(|col| (*col).to_string()).collect()),
                Some(schema_of_file),
                scan_task.pushdowns.filters.clone(),
            );
            let parse_options = CsvParseOptions::new_with_defaults(
                cfg.has_headers,
                cfg.delimiter,
                cfg.double_quote,
                cfg.quote,
                cfg.allow_variable_columns,
                cfg.escape_char,
                cfg.comment,
            )?;
            let read_options = CsvReadOptions::new_internal(cfg.buffer_size, cfg.chunk_size);
            daft_csv::stream_csv(
                url.to_string(),
                Some(convert_options),
                Some(parse_options),
                Some(read_options),
                io_client,
                Some(io_stats.clone()),
                None,
                // maintain_order, TODO: Implement maintain_order for CSV
            )
            .await?
        }
        FileFormatConfig::Json(cfg) => {
            let schema_of_file = scan_task.schema.clone();
            let convert_options = JsonConvertOptions::new_internal(
                scan_task.pushdowns.limit,
                file_column_names
                    .as_ref()
                    .map(|cols| cols.iter().map(|col| (*col).to_string()).collect()),
                Some(schema_of_file),
                scan_task.pushdowns.filters.clone(),
            );
            let parse_options = JsonParseOptions::new_internal();
            let read_options = JsonReadOptions::new_internal(cfg.buffer_size, cfg.chunk_size);

            daft_json::read::stream_json(
                url.to_string(),
                Some(convert_options),
                Some(parse_options),
                Some(read_options),
                io_client,
                Some(io_stats),
                None,
                // maintain_order, TODO: Implement maintain_order for JSON
            )
            .await?
        }
        FileFormatConfig::Warc(_) => {
            let convert_options = WarcConvertOptions {
                limit: scan_task.pushdowns.limit,
                include_columns: None,
                schema: scan_task.schema.clone(),
                predicate: scan_task.pushdowns.filters.clone(),
            };
            daft_warc::stream_warc(url, io_client, Some(io_stats), convert_options, None).await?
        }
        #[cfg(feature = "python")]
        FileFormatConfig::Database(common_file_formats::DatabaseSourceConfig { sql, conn }) => {
            use pyo3::Python;

            use crate::PyIOSnafu;
            let predicate = scan_task
                .pushdowns
                .filters
                .as_ref()
                .map(|p| (*p.as_ref()).clone().into());
            let table = Python::with_gil(|py| {
                daft_micropartition::python::read_sql_into_py_table(
                    py,
                    sql,
                    conn,
                    predicate.clone(),
                    scan_task.schema.clone().into(),
                    scan_task
                        .pushdowns
                        .columns
                        .as_ref()
                        .map(|cols| cols.as_ref().clone()),
                    scan_task.pushdowns.limit,
                )
                .map(|t| t.into())
                .context(PyIOSnafu)
            })?;
            Box::pin(futures::stream::once(async { Ok(table) }))
        }
        #[cfg(feature = "python")]
        FileFormatConfig::PythonFunction => {
            let iter = daft_micropartition::python::read_pyfunc_into_table_iter(&scan_task)?;
            let stream = futures::stream::iter(iter.map(|r| r.map_err(|e| e.into())));
            Box::pin(stream)
        }
    };

    Ok(table_stream.map(move |table| {
        let table = table?;
        let casted_table = table.cast_to_schema_with_fill(
            scan_task.materialized_schema().as_ref(),
            scan_task
                .partition_spec()
                .as_ref()
                .map(|pspec| pspec.to_fill_map())
                .as_ref(),
        )?;
        let mp = Arc::new(MicroPartition::new_loaded(
            scan_task.materialized_schema(),
            Arc::new(vec![casted_table]),
            scan_task.statistics.clone(),
        ));
        Ok(mp)
    }))
}
