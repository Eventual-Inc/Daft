use common_error::DaftResult;
use daft_core::schema::SchemaRef;
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_io::{IOStatsContext, IOStatsRef};
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_micropartition::MicroPartition;
use daft_parquet::read::ParquetSchemaInferenceOptions;
use daft_scan::{
    file_format::{FileFormatConfig, ParquetSourceConfig},
    storage_config::StorageConfig,
    ChunkSpec, ScanTask,
};
use daft_stats::{PartitionSpec, TableStatistics};
use daft_table::Table;
use futures::{stream::BoxStream, StreamExt};
use std::sync::Arc;

use crate::DEFAULT_MORSEL_SIZE;

use super::source::{Source, SourceStream};

use tracing::instrument;

#[derive(Debug)]
pub struct ScanTaskSource {
    scan_task: Arc<ScanTask>,
}

impl ScanTaskSource {
    pub fn new(scan_task: Arc<ScanTask>) -> Self {
        Self { scan_task }
    }

    pub fn boxed(self) -> Box<dyn Source> {
        Box::new(self) as Box<dyn Source>
    }
}

impl Source for ScanTaskSource {
    #[instrument(name = "ScanTaskSource::get_data", level = "info", skip(self))]
    fn get_data(&self, maintain_order: bool) -> SourceStream {
        let morsel_size = DEFAULT_MORSEL_SIZE;
        let io_stats = IOStatsContext::new("StreamScanTask");
        let stream = async_stream::try_stream! {
            let mut stream =
                stream_scan_task(self.scan_task.clone(), Some(io_stats), maintain_order, morsel_size).await?;
            while let Some(partition) = stream.next().await {
                yield partition?;
            }
        };
        Box::pin(stream)
    }
}

async fn stream_scan_task(
    scan_task: Arc<ScanTask>,
    io_stats: Option<IOStatsRef>,
    maintain_order: bool,
    morsel_size: usize,
) -> DaftResult<SourceStream<'static>> {
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
            let io_client = daft_io::get_io_client_async(multi_threaded_io, io_config).await?;

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

                    if source.get_iceberg_delete_files().is_some() {
                        return Err(common_error::DaftError::TypeError(
                            "Streaming reads not supported for Iceberg delete files".to_string(),
                        ));
                    }

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

    let mp_stream = chunk_tables_into_micropartition_stream(
        table_stream,
        scan_task.materialized_schema(),
        scan_task.partition_spec().cloned(),
        scan_task.statistics.clone(),
        morsel_size,
    );
    Ok(Box::pin(mp_stream))
}

fn chunk_tables_into_micropartition_stream(
    mut table_stream: BoxStream<'static, DaftResult<Table>>,
    schema: SchemaRef,
    partition_spec: Option<PartitionSpec>,
    statistics: Option<TableStatistics>,
    morsel_size: usize,
) -> SourceStream<'static> {
    let chunked_stream = async_stream::try_stream! {
        let mut buffer = vec![];
        let mut total_rows = 0;
        while let Some(table) = table_stream.next().await {
            let table = table?;
            let casted_table = table.cast_to_schema_with_fill(schema.as_ref(), partition_spec.as_ref().map(|pspec| pspec.to_fill_map()).as_ref())?;
            total_rows += casted_table.len();
            buffer.push(casted_table);

            if total_rows >= morsel_size {
                let mp = Arc::new(MicroPartition::new_loaded(schema.clone(), Arc::new(buffer), statistics.clone()));
                buffer = vec![];
                total_rows = 0;
                yield mp;
            }
        }
        if !buffer.is_empty() {
            let mp = Arc::new(MicroPartition::new_loaded(schema, Arc::new(buffer), statistics));
            yield mp;
        }
    };
    Box::pin(chunked_stream)
}
