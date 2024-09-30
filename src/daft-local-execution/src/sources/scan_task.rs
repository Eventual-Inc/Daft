use std::sync::Arc;

use common_error::DaftResult;
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use daft_core::prelude::{Int64Array, Utf8Array};
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_io::{get_runtime, IOClient, IOStatsRef};
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_micropartition::MicroPartition;
use daft_parquet::read::{read_parquet_bulk_async, ParquetSchemaInferenceOptions};
use daft_scan::{storage_config::StorageConfig, ChunkSpec, ScanTask};
use futures::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use tracing::instrument;

use crate::{
    channel::{create_channel, Sender},
    sources::source::{Source, SourceStream},
    ExecutionRuntimeHandle, NUM_CPUS,
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
        let (senders, receivers): (Vec<_>, Vec<_>) = match maintain_order {
            true => (0..self.scan_tasks.len())
                .map(|_| create_channel(1))
                .unzip(),
            false => {
                let (sender, receiver) = create_channel(self.scan_tasks.len());
                (
                    std::iter::repeat(sender)
                        .take(self.scan_tasks.len())
                        .collect(),
                    vec![receiver],
                )
            }
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

fn _read_delete_files(
    delete_files: &[String],
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
    multithreaded_io: bool,
    schema_infer_options: &ParquetSchemaInferenceOptions,
) -> DaftResult<Vec<i64>> {
    let columns = Some(vec!["file_path".to_string(), "pos".to_string()]);
    let schema_infer_options = *schema_infer_options;
    let delete_files = delete_files.to_owned();
    let runtime = get_runtime(multithreaded_io)?;
    let tables = runtime.block_on_io_pool(async move {
        read_parquet_bulk_async(
            delete_files,
            columns,
            None,
            None,
            None,
            None,
            io_client,
            io_stats,
            num_parallel_tasks,
            schema_infer_options,
            None,
            None,
            None,
            None,
        )
        .await
    })??;

    let mut delete_rows = vec![];
    for table_result in tables.into_iter() {
        let table = table_result?;
        // values in the file_path column are guaranteed by the iceberg spec to match the full URI of the corresponding data file
        // https://iceberg.apache.org/spec/#position-delete-files
        let file_paths = table.get_column("file_path")?.downcast::<Utf8Array>()?;
        let positions = table.get_column("pos")?.downcast::<Int64Array>()?;

        for i in 0..table.len() {
            let file = file_paths.get(i);
            let pos = positions.get(i);

            if let Some(file) = file
                && let Some(pos) = pos
                && file == uri
            {
                delete_rows.push(pos);
            }
        }
    }

    Ok(delete_rows)
}

async fn stream_scan_task(
    scan_task: Arc<ScanTask>,
    io_stats: Option<IOStatsRef>,
    maintain_order: bool,
) -> DaftResult<impl Stream<Item = DaftResult<Arc<MicroPartition>>> + Send> {
    let pushdown_columns = scan_task
        .pushdowns
        .columns
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<&str>>());

    let file_column_names = match (
        pushdown_columns,
        scan_task.partition_spec().map(|ps| ps.to_fill_map()),
    ) {
        (None, _) => None,
        (Some(columns), None) => Some(columns.to_vec()),

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
    let table_stream = match scan_task.storage_config.as_ref() {
        StorageConfig::Native(native_storage_config) => {
            let io_config = Arc::new(
                native_storage_config
                    .io_config
                    .as_ref()
                    .cloned()
                    .unwrap_or_default(),
            );
            let multi_threaded_io = native_storage_config.multithreaded_io;
            let io_client = daft_io::get_io_client(multi_threaded_io, io_config)?;

            match scan_task.file_format_config.as_ref() {
                // ********************
                // Native Parquet Reads
                // ********************
                FileFormatConfig::Parquet(ParquetSourceConfig {
                    coerce_int96_timestamp_unit,
                    field_id_mapping,
                    ..
                }) => {
                    let inference_options =
                        ParquetSchemaInferenceOptions::new(Some(*coerce_int96_timestamp_unit));

                    let delete_rows = if let Some(delete_files) = source.get_iceberg_delete_files()
                    {
                        Some(_read_delete_files(
                            delete_files,
                            url,
                            io_client.clone(),
                            io_stats.clone(),
                            *NUM_CPUS,
                            multi_threaded_io,
                            &inference_options,
                        )?)
                    } else {
                        None
                    };

                    let row_groups =
                        if let Some(ChunkSpec::Parquet(row_groups)) = source.get_chunk_spec() {
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
                        io_client.clone(),
                        io_stats,
                        &inference_options,
                        field_id_mapping.clone(),
                        metadata,
                        maintain_order,
                        delete_rows,
                    )
                    .await?
                }

                // ****************
                // Native CSV Reads
                // ****************
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
                            .map(|cols| cols.iter().map(|col| col.to_string()).collect()),
                        col_names
                            .as_ref()
                            .map(|cols| cols.iter().map(|col| col.to_string()).collect()),
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
                    let read_options =
                        CsvReadOptions::new_internal(cfg.buffer_size, cfg.chunk_size);
                    daft_csv::stream_csv(
                        url.to_string(),
                        Some(convert_options),
                        Some(parse_options),
                        Some(read_options),
                        io_client.clone(),
                        io_stats.clone(),
                        None,
                        // maintain_order, TODO: Implement maintain_order for CSV
                    )
                    .await?
                }

                // ****************
                // Native JSON Reads
                // ****************
                FileFormatConfig::Json(cfg) => {
                    let schema_of_file = scan_task.schema.clone();
                    let convert_options = JsonConvertOptions::new_internal(
                        scan_task.pushdowns.limit,
                        file_column_names
                            .as_ref()
                            .map(|cols| cols.iter().map(|col| col.to_string()).collect()),
                        Some(schema_of_file),
                        scan_task.pushdowns.filters.clone(),
                    );
                    // let
                    let parse_options = JsonParseOptions::new_internal();
                    let read_options =
                        JsonReadOptions::new_internal(cfg.buffer_size, cfg.chunk_size);

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
                FileFormatConfig::Database(_) => {
                    return Err(common_error::DaftError::TypeError(
                        "Native reads for Database file format not implemented".to_string(),
                    ));
                }
                #[cfg(feature = "python")]
                FileFormatConfig::PythonFunction => {
                    return Err(common_error::DaftError::TypeError(
                        "Native reads for PythonFunction file format not implemented".to_string(),
                    ));
                }
            }
        }
        #[cfg(feature = "python")]
        StorageConfig::Python(_) => {
            return Err(common_error::DaftError::TypeError(
                "Streaming reads not supported for Python storage config".to_string(),
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
            scan_task.materialized_schema().clone(),
            Arc::new(vec![casted_table]),
            scan_task.statistics.clone(),
        ));
        Ok(mp)
    }))
}
