use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_dsl::{AggExpr, Expr};
use daft_io::{GetRange, IOStatsRef};
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_parquet::read::ParquetSchemaInferenceOptions;
use daft_recordbatch::RecordBatch;
use daft_scan::{
    ChunkSpec, CsvSourceConfig, FileFormatConfig, JsonSourceConfig, ParquetSourceConfig, ScanTask,
    SourceConfig, TextSourceConfig,
};
use daft_text::{TextConvertOptions, TextReadOptions};
use daft_warc::WarcConvertOptions;
use futures::stream::BoxStream;

/// Dispatches a ScanTask to the appropriate reader based on its SourceConfig,
/// returning a stream of RecordBatches.
///
/// Post-processing (schema conformance, partition fill) is handled by the caller.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_scan_task(
    scan_task: &Arc<ScanTask>,
    url: &str,
    file_column_names: Option<Vec<String>>,
    io_client: Arc<daft_io::IOClient>,
    io_stats: IOStatsRef,
    delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
    maintain_order: bool,
    chunk_size: usize,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    match scan_task.source_config.as_ref() {
        SourceConfig::File(ffc) => match ffc {
            FileFormatConfig::Parquet(cfg) => {
                read_parquet(
                    scan_task,
                    cfg,
                    url,
                    file_column_names,
                    io_client,
                    io_stats,
                    delete_map,
                    maintain_order,
                    chunk_size,
                )
                .await
            }
            FileFormatConfig::Csv(cfg) => {
                read_csv(
                    scan_task,
                    cfg,
                    url,
                    file_column_names,
                    io_client,
                    io_stats,
                    chunk_size,
                )
                .await
            }
            FileFormatConfig::Json(cfg) => {
                read_json(
                    scan_task,
                    cfg,
                    url,
                    file_column_names,
                    io_client,
                    io_stats,
                    chunk_size,
                )
                .await
            }
            FileFormatConfig::Warc(_) => read_warc(scan_task, url, io_client, io_stats).await,
            FileFormatConfig::Text(cfg) => {
                read_text(scan_task, cfg, url, io_client, io_stats, chunk_size).await
            }
        },
        #[cfg(feature = "python")]
        SourceConfig::Database(cfg) => read_database(scan_task, cfg).await,
        #[cfg(feature = "python")]
        SourceConfig::PythonFunction { .. } => read_python_function(scan_task).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_parquet(
    scan_task: &Arc<ScanTask>,
    cfg: &ParquetSourceConfig,
    url: &str,
    file_column_names: Option<Vec<String>>,
    io_client: Arc<daft_io::IOClient>,
    io_stats: IOStatsRef,
    delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
    maintain_order: bool,
    chunk_size: usize,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let source = scan_task.sources.first().unwrap();

    if let Some(aggregation) = &scan_task.pushdowns.aggregation
        && let Expr::Agg(AggExpr::Count(_, _)) = aggregation.as_ref()
    {
        daft_parquet::read::stream_parquet_count_pushdown(
            url,
            io_client,
            Some(io_stats),
            cfg.field_id_mapping.clone(),
            aggregation,
        )
        .await
    } else {
        let parquet_chunk_size = cfg.chunk_size.or(Some(chunk_size));
        let inference_options =
            ParquetSchemaInferenceOptions::new(Some(cfg.coerce_int96_timestamp_unit));

        let delete_rows = delete_map.as_ref().and_then(|m| m.get(url).cloned());
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
            file_column_names,
            scan_task.pushdowns.limit,
            row_groups,
            scan_task.pushdowns.filters.clone(),
            io_client,
            Some(io_stats),
            &inference_options,
            cfg.field_id_mapping.clone(),
            metadata,
            maintain_order,
            delete_rows,
            parquet_chunk_size,
        )
        .await
    }
}

async fn read_csv(
    scan_task: &ScanTask,
    cfg: &CsvSourceConfig,
    url: &str,
    file_column_names: Option<Vec<String>>,
    io_client: Arc<daft_io::IOClient>,
    io_stats: IOStatsRef,
    chunk_size: usize,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let schema_of_file = scan_task.schema.clone();
    let col_names = if !cfg.has_headers {
        Some(schema_of_file.field_names().collect::<Vec<_>>())
    } else {
        None
    };
    let convert_options = CsvConvertOptions::new_internal(
        scan_task.pushdowns.limit,
        file_column_names
            .as_ref()
            .map(|cols| cols.iter().map(|col| (*col).clone()).collect()),
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
    let csv_chunk_size = cfg.chunk_size.or(Some(chunk_size));
    let read_options = CsvReadOptions::new_internal(cfg.buffer_size, csv_chunk_size);
    daft_csv::stream_csv(
        url.to_string(),
        Some(convert_options),
        Some(parse_options),
        Some(read_options),
        io_client,
        Some(io_stats),
        None,
    )
    .await
}

async fn read_json(
    scan_task: &ScanTask,
    cfg: &JsonSourceConfig,
    url: &str,
    file_column_names: Option<Vec<String>>,
    io_client: Arc<daft_io::IOClient>,
    io_stats: IOStatsRef,
    chunk_size: usize,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let source = scan_task.sources.first().unwrap();
    let schema_of_file = scan_task.schema.clone();
    let convert_options = JsonConvertOptions::new_internal(
        scan_task.pushdowns.limit,
        file_column_names
            .as_ref()
            .map(|cols| cols.iter().map(|col| (*col).clone()).collect()),
        Some(schema_of_file),
        scan_task.pushdowns.filters.clone(),
    );
    let parse_options = JsonParseOptions::new_internal(cfg.skip_empty_files);
    let json_chunk_size = cfg.chunk_size.or(Some(chunk_size));
    let read_options = JsonReadOptions::new_internal(cfg.buffer_size, json_chunk_size);

    let range = source.get_chunk_spec().and_then(|spec| match spec {
        daft_scan::ChunkSpec::Bytes { start, end } => Some(GetRange::Bounded(*start..*end)),
        _ => None,
    });
    daft_json::read::stream_json(
        url.to_string(),
        Some(convert_options),
        Some(parse_options),
        Some(read_options),
        io_client,
        Some(io_stats),
        None,
        range,
    )
    .await
}

async fn read_warc(
    scan_task: &ScanTask,
    url: &str,
    io_client: Arc<daft_io::IOClient>,
    io_stats: IOStatsRef,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let convert_options = WarcConvertOptions {
        limit: scan_task.pushdowns.limit,
        include_columns: None,
        schema: scan_task.schema.clone(),
        predicate: scan_task.pushdowns.filters.clone(),
    };
    daft_warc::stream_warc(url, io_client, io_stats, convert_options, None).await
}

async fn read_text(
    scan_task: &ScanTask,
    cfg: &TextSourceConfig,
    url: &str,
    io_client: Arc<daft_io::IOClient>,
    io_stats: IOStatsRef,
    chunk_size: usize,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let schema_of_file = scan_task.schema.clone();
    let convert_options = TextConvertOptions::new(
        &cfg.encoding,
        cfg.skip_blank_lines,
        cfg.whole_text,
        Some(schema_of_file),
        scan_task.pushdowns.limit,
    );
    let text_chunk_size = cfg.chunk_size.or(Some(chunk_size));
    let read_options = TextReadOptions::new(cfg.buffer_size, text_chunk_size);
    daft_text::read::stream_text(
        url.to_string(),
        convert_options,
        read_options,
        io_client,
        Some(io_stats),
    )
    .await
}

#[cfg(feature = "python")]
async fn read_database(
    scan_task: &ScanTask,
    cfg: &daft_scan::DatabaseSourceConfig,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    use pyo3::Python;
    use snafu::ResultExt;

    use crate::PyIOSnafu;

    let predicate = scan_task
        .pushdowns
        .filters
        .as_ref()
        .map(|p| (*p.as_ref()).clone().into());
    let table: RecordBatch = Python::attach(|py| {
        daft_micropartition::python::read_sql_into_py_table(
            py,
            &cfg.sql,
            &cfg.conn,
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
    Ok(Box::pin(futures::stream::once(async { Ok(table) })))
}

#[cfg(feature = "python")]
async fn read_python_function(
    scan_task: &Arc<ScanTask>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let iter = daft_micropartition::python::read_pyfunc_into_table_iter(scan_task.clone())?;
    let stream = futures::stream::iter(iter.map(|r| r.map_err(|e| e.into())));
    Ok(Box::pin(stream))
}
