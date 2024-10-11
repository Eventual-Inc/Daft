use std::sync::Arc;

use common_error::DaftResult;
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_io::IOStatsRef;
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_micropartition::MicroPartition;
use daft_parquet::read::ParquetSchemaInferenceOptions;
use daft_scan::{storage_config::StorageConfig, ChunkSpec, ScanTask};
use futures::{Stream, StreamExt};
use snafu::ResultExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::instrument;

use crate::{
    channel::{create_channel, Sender},
    sources::source::{Source, SourceStream},
    ExecutionRuntimeHandle,
};

pub struct ScanTaskSource {
    scan_tasks: Vec<Arc<ScanTask>>,
}

impl ScanTaskSource {
    pub fn new(scan_tasks: Vec<Arc<ScanTask>>) -> Self {
        Self { scan_tasks }
    }

    #[instrument(
        name = "ScanTaskSource::process_scan_task_stream",
        level = "info",
        skip_all
    )]
    async fn process_scan_task_stream(
        scan_task: Arc<ScanTask>,
        sender: Sender<Arc<MicroPartition>>,
        maintain_order: bool,
        io_stats: IOStatsRef,
    ) -> DaftResult<()> {
        let schema = scan_task.materialized_schema();
        let mut stream = stream_scan_task(scan_task, Some(io_stats), maintain_order).await?;
        let mut has_data = false;
        while let Some(partition) = stream.next().await {
            let _ = sender.send(partition?).await;
            has_data = true;
        }
        if !has_data {
            let empty = Arc::new(MicroPartition::empty(Some(schema.clone())));
            let _ = sender.send(empty).await;
        }
        Ok(())
    }
    pub fn boxed(self) -> Box<dyn Source> {
        Box::new(self) as Box<dyn Source>
    }
}
impl Source for ScanTaskSource {
    #[instrument(name = "ScanTaskSource::get_data", level = "info", skip_all)]
    fn get_data(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
        io_stats: IOStatsRef,
    ) -> crate::Result<SourceStream<'static>> {
        let (senders, receivers): (Vec<_>, Vec<_>) = if maintain_order {
            (0..self.scan_tasks.len())
                .map(|_| create_channel(1))
                .unzip()
        } else {
            let (sender, receiver) = create_channel(self.scan_tasks.len());
            (
                std::iter::repeat(sender)
                    .take(self.scan_tasks.len())
                    .collect(),
                vec![receiver],
            )
        };
        for (scan_task, sender) in self.scan_tasks.iter().zip(senders) {
            runtime_handle.spawn(
                Self::process_scan_task_stream(
                    scan_task.clone(),
                    sender,
                    maintain_order,
                    io_stats.clone(),
                ),
                self.name(),
            );
        }
        let stream = futures::stream::iter(receivers.into_iter().map(ReceiverStream::new));
        Ok(Box::pin(stream.flatten()))
    }

    fn name(&self) -> &'static str {
        "ScanTask"
    }
}

async fn stream_scan_task(
    scan_task: Arc<ScanTask>,
    io_stats: Option<IOStatsRef>,
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
    let (io_config, multi_threaded_io) = match scan_task.storage_config.as_ref() {
        StorageConfig::Native(native_storage_config) => (
            native_storage_config.io_config.as_ref(),
            native_storage_config.multithreaded_io,
        ),

        #[cfg(feature = "python")]
        StorageConfig::Python(python_storage_config) => {
            (python_storage_config.io_config.as_ref(), true)
        }
    };
    let io_config = Arc::new(io_config.cloned().unwrap_or_default());
    let io_client = daft_io::get_io_client(multi_threaded_io, io_config)?;
    let table_stream = match scan_task.file_format_config.as_ref() {
        FileFormatConfig::Parquet(ParquetSourceConfig {
            coerce_int96_timestamp_unit,
            field_id_mapping,
            ..
        }) => {
            let inference_options =
                ParquetSchemaInferenceOptions::new(Some(*coerce_int96_timestamp_unit));

            if source.get_iceberg_delete_files().is_some() {
                return Err(common_error::DaftError::TypeError(
                    "Streaming reads not supported for Iceberg delete files".to_string(),
                ));
            }

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
                None,
                scan_task.pushdowns.limit,
                row_groups,
                scan_task.pushdowns.filters.clone(),
                io_client,
                io_stats,
                &inference_options,
                field_id_mapping.clone(),
                metadata,
                maintain_order,
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
                io_stats.clone(),
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
                io_stats,
                None,
                // maintain_order, TODO: Implement maintain_order for JSON
            )
            .await?
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
            // SQL Scan cannot be streamed at the moment, so we just return the table
            Box::pin(futures::stream::once(async { Ok(table) }))
        }
        #[cfg(feature = "python")]
        FileFormatConfig::PythonFunction => {
            return Err(common_error::DaftError::TypeError(
                "PythonFunction file format not implemented".to_string(),
            ));
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
