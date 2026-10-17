use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_dsl::{AggExpr, Expr};
use daft_io::{GetRange, IOStatsRef};
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_parquet::read::{ParquetReadOptions, ParquetSchemaInferenceOptions};
use daft_recordbatch::RecordBatch;
use daft_scan::{
    ChunkSpec, CsvSourceConfig, FileFormatConfig, JsonSourceConfig, McapSourceConfig,
    ParquetSourceConfig, ScanTask, SourceConfig, TextSourceConfig,
};
use daft_text::{TextConvertOptions, TextReadOptions};
use daft_warc::WarcConvertOptions;
use futures::stream::BoxStream;

type SkippedCorruptFilesCollector = Option<Arc<std::sync::Mutex<Vec<(String, String, bool)>>>>;

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
    skipped_corrupt_files: SkippedCorruptFilesCollector,
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
                    skipped_corrupt_files,
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
                    skipped_corrupt_files,
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
            FileFormatConfig::Avro(_cfg) => read_avro(scan_task, url, io_client, io_stats).await,
            FileFormatConfig::Mcap(cfg) => {
                read_mcap(scan_task, cfg, url, file_column_names, io_client, io_stats).await
            }
        },
        #[cfg(feature = "python")]
        SourceConfig::Database(cfg) => read_database(scan_task, cfg).await,
        #[cfg(feature = "python")]
        SourceConfig::PythonFunction { .. } => {
            let orc = OrcReadContext::from_scan_task(scan_task)?;
            let stream = read_python_function(scan_task).await?;
            match orc {
                Some(orc) => Ok(orc.filter_stream(stream, skipped_corrupt_files)),
                None => Ok(stream),
            }
        }
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
    // Unused: the arrow-rs reader is always file-ordered within a single file
    // (see `build_rg_stream` in daft-parquet's reader). `maintain_order=false`
    // at the scan layer reorders only BETWEEN scan tasks, which is handled by
    // the caller, not here.
    _maintain_order: bool,
    chunk_size: usize,
    skipped_corrupt_files: SkippedCorruptFilesCollector,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let source = scan_task.sources.first().unwrap();

    let row_groups = match source.get_chunk_spec() {
        Some(ChunkSpec::Parquet(rgs)) => Some(rgs.clone()),
        _ => None,
    };

    if let Some(aggregation) = &scan_task.pushdowns.aggregation
        && let Expr::Agg(AggExpr::Count(_, count_mode)) = aggregation.as_ref()
    {
        // The optimizer guarantees built-in sources never reach here with row-level
        // processing. Error loudly if a custom ScanOperator breaks that: falling back
        // isn't safe, since the post-pushdown schema is a single count field and
        // `SUM` over a null-filled batch yields null instead of a count.
        let has_deletes = delete_map
            .as_ref()
            .and_then(|m| m.get(url))
            .is_some_and(|d| !d.is_empty());
        if let Some(reason) =
            parquet_count_pushdown_unsupported_reason(count_mode, &scan_task.pushdowns, has_deletes)
        {
            return Err(common_error::DaftError::InternalError(reason));
        }

        return count_pushdown_stream(
            url,
            io_client,
            io_stats,
            cfg.field_id_mapping.clone(),
            aggregation,
            row_groups,
        )
        .await;
    }

    let opts = ParquetReadOptions {
        columns: file_column_names,
        num_rows: scan_task.pushdowns.limit,
        row_groups,
        predicate: scan_task.pushdowns.filters.clone(),
        schema_infer: ParquetSchemaInferenceOptions::new(Some(cfg.coerce_int96_timestamp_unit)),
        field_id_mapping: cfg.field_id_mapping.clone(),
        delete_rows: delete_map.as_ref().and_then(|m| m.get(url).cloned()),
        batch_size: cfg.chunk_size.or(Some(chunk_size)),
        metadata: scan_task
            .sources
            .first()
            .and_then(|s| s.get_parquet_metadata().cloned()),
        ignore_corrupt_files: cfg.ignore_corrupt_files,
        skipped_corrupt_files: skipped_corrupt_files.clone(),
        ..Default::default()
    };
    // Box::pin: setup future is large (~20KB) due to many tuning args.
    Box::pin(daft_parquet::read::read_parquet(
        url,
        io_client,
        Some(io_stats),
        opts,
    ))
    .await
}

/// Returns `Some(reason)` if the count can't be served from parquet metadata alone.
/// A non-`All` count mode, filter, limit, or delete rows all require row-level
/// processing the metadata shortcut can't do.
fn parquet_count_pushdown_unsupported_reason(
    count_mode: &daft_core::count_mode::CountMode,
    pushdowns: &daft_scan::Pushdowns,
    has_deletes: bool,
) -> Option<String> {
    if !matches!(count_mode, daft_core::count_mode::CountMode::All) {
        return Some(format!(
            "unsupported count mode for parquet count pushdown: {count_mode:?}"
        ));
    }
    if pushdowns.filters.is_some() {
        return Some("parquet metadata count pushdown cannot apply row-level filters".to_string());
    }
    if pushdowns.limit.is_some() {
        return Some("parquet metadata count pushdown cannot apply a limit".to_string());
    }
    if has_deletes {
        return Some("parquet metadata count pushdown cannot apply delete rows".to_string());
    }
    None
}

async fn count_pushdown_stream(
    url: &str,
    io_client: Arc<daft_io::IOClient>,
    io_stats: IOStatsRef,
    field_id_mapping: Option<Arc<std::collections::BTreeMap<i32, daft_core::prelude::Field>>>,
    aggregation: &daft_dsl::ExprRef,
    row_groups: Option<Vec<i64>>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    daft_parquet::read::stream_parquet_count_pushdown(
        url,
        io_client,
        Some(io_stats),
        field_id_mapping,
        aggregation,
        row_groups.as_deref(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn read_csv(
    scan_task: &ScanTask,
    cfg: &CsvSourceConfig,
    url: &str,
    file_column_names: Option<Vec<String>>,
    io_client: Arc<daft_io::IOClient>,
    io_stats: IOStatsRef,
    chunk_size: usize,
    skipped_corrupt_files: SkippedCorruptFilesCollector,
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
        cfg.ignore_corrupt_files,
        skipped_corrupt_files,
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

async fn read_mcap(
    scan_task: &ScanTask,
    cfg: &McapSourceConfig,
    url: &str,
    file_column_names: Option<Vec<String>>,
    io_client: Arc<daft_io::IOClient>,
    io_stats: IOStatsRef,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let read_options = daft_mcap::McapReadOptions {
        batch_size: cfg.batch_size,
        start_time: cfg.start_time,
        end_time: cfg.end_time,
        topics: cfg.topics.clone(),
    };
    let convert_options = daft_mcap::McapConvertOptions {
        predicate: scan_task.pushdowns.filters.clone(),
        include_columns: file_column_names,
        limit: scan_task.pushdowns.limit,
    };
    daft_mcap::stream_mcap(url, io_client, io_stats, read_options, convert_options).await
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

async fn read_avro(
    scan_task: &ScanTask,
    url: &str,
    io_client: Arc<daft_io::IOClient>,
    io_stats: IOStatsRef,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let column_projection = scan_task.pushdowns.columns.as_deref().cloned();
    let max_records = scan_task.pushdowns.limit;

    let record_batch = daft_avro::read::read_avro(
        url,
        io_client,
        Some(io_stats),
        column_projection,
        max_records,
    )
    .await?;

    Ok(Box::pin(futures::stream::once(async { Ok(record_batch) })))
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
struct OrcReadContext {
    path: String,
    classify_error: pyo3::Py<pyo3::PyAny>,
}

#[cfg(feature = "python")]
impl OrcReadContext {
    fn from_scan_task(scan_task: &ScanTask) -> DaftResult<Option<Self>> {
        use daft_scan::ScanSourceKind;
        use pyo3::prelude::*;

        let Some(source) = scan_task.sources.first() else {
            return Ok(None);
        };
        let ScanSourceKind::PythonFactoryFunction {
            module,
            func_name,
            func_args,
        } = &source.kind
        else {
            return Ok(None);
        };
        if module != "daft.io.__internal" || func_name != "_get_record_batches" {
            return Ok(None);
        }
        Python::attach(|py| -> PyResult<Option<Self>> {
            let args = func_args.to_pytuple(py)?;
            if args.len() != 1 {
                return Ok(None);
            }
            let task = args.get_item(0)?;
            let orc = py.import("daft.io._orc")?;
            if !task.get_type().as_any().is(&orc.getattr("OrcSourceTask")?) {
                return Ok(None);
            }
            if !task.getattr("_ignore_corrupt_files")?.extract::<bool>()? {
                return Ok(None);
            }
            Ok(Some(Self {
                path: task.getattr("_path")?.extract()?,
                classify_error: orc.getattr("_is_ignorable_orc_error")?.unbind(),
            }))
        })
        .map_err(Into::into)
    }

    fn is_ignorable(&self, error: &common_error::DaftError) -> DaftResult<bool> {
        use common_error::DaftError;
        use pyo3::prelude::*;

        let DaftError::External(error) = error else {
            return Ok(false);
        };
        let Some(daft_micropartition::Error::PyIO { source }) =
            error.downcast_ref::<daft_micropartition::Error>()
        else {
            return Ok(false);
        };
        Python::attach(|py| {
            self.classify_error
                .call1(py, (source.value(py),))?
                .extract::<bool>(py)
        })
        .map_err(Into::into)
    }

    fn filter_stream(
        self,
        stream: BoxStream<'static, DaftResult<RecordBatch>>,
        skipped_corrupt_files: SkippedCorruptFilesCollector,
    ) -> BoxStream<'static, DaftResult<RecordBatch>> {
        use futures::StreamExt;

        let mut saw_ok = false;
        Box::pin(stream.filter_map(move |result| {
            let filtered = match result {
                Ok(batch) => {
                    saw_ok = true;
                    Some(Ok(batch))
                }
                Err(error) => match self.is_ignorable(&error) {
                    Ok(true) => {
                        match record_orc_skip(
                            &skipped_corrupt_files,
                            &self.path,
                            &error.to_string(),
                            saw_ok,
                        ) {
                            Ok(()) => None,
                            Err(error) => Some(Err(error)),
                        }
                    }
                    Ok(false) => Some(Err(error)),
                    Err(classification_error) => Some(Err(classification_error)),
                },
            };
            futures::future::ready(filtered)
        }))
    }
}

#[cfg(any(feature = "python", test))]
fn record_orc_skip(
    collector: &SkippedCorruptFilesCollector,
    path: &str,
    reason: &str,
    partial: bool,
) -> DaftResult<()> {
    if let Some(collector) = collector {
        collector
            .lock()
            .map_err(|_| {
                common_error::DaftError::InternalError(
                    "Unable to record skipped ORC file".to_string(),
                )
            })?
            .push((path.to_string(), reason.to_string(), partial));
    }
    tracing::warn!("Skipping corrupt ORC file {path} (partial={partial}): {reason}");
    Ok(())
}

#[cfg(feature = "python")]
async fn read_python_function(
    scan_task: &Arc<ScanTask>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let iter = daft_micropartition::python::read_pyfunc_into_table_iter(scan_task.clone())?;
    let stream = futures::stream::iter(iter.map(|r| r.map_err(|e| e.into())));
    Ok(Box::pin(stream))
}

#[cfg(test)]
mod tests {
    use daft_core::count_mode::CountMode;
    use daft_scan::Pushdowns;

    use super::parquet_count_pushdown_unsupported_reason;

    #[test]
    fn orc_skip_preserves_shared_entries_and_partial() {
        let collector = std::sync::Arc::new(std::sync::Mutex::new(vec![(
            "existing.parquet".to_string(),
            "existing failure".to_string(),
            false,
        )]));
        super::record_orc_skip(
            &Some(collector.clone()),
            "partial.orc",
            "decode failure",
            true,
        )
        .unwrap();
        let entries = collector.lock().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "existing.parquet");
        assert_eq!(
            entries[1],
            (
                "partial.orc".to_string(),
                "decode failure".to_string(),
                true
            )
        );
    }

    #[test]
    fn orc_skip_collector_failure_is_visible() {
        let collector = std::sync::Arc::new(std::sync::Mutex::new(vec![]));
        let poisoned = std::panic::catch_unwind(|| {
            let _guard = collector.lock().unwrap();
            std::panic::resume_unwind(Box::new(()));
        });
        assert!(poisoned.is_err());
        let result = super::record_orc_skip(&Some(collector), "bad.orc", "decode failure", false);
        assert!(matches!(
            result,
            Err(common_error::DaftError::InternalError(_))
        ));
    }

    #[test]
    fn count_all_no_pushdowns_is_supported() {
        let reason = parquet_count_pushdown_unsupported_reason(
            &CountMode::All,
            &Pushdowns::default(),
            false,
        );
        assert_eq!(reason, None);
    }

    #[test]
    fn non_all_count_mode_errors() {
        for mode in [CountMode::Valid, CountMode::Null] {
            let reason =
                parquet_count_pushdown_unsupported_reason(&mode, &Pushdowns::default(), false);
            assert!(
                reason.is_some_and(|r| r.contains("count mode")),
                "expected count-mode error for {mode:?}"
            );
        }
    }

    #[test]
    fn filter_errors() {
        let pushdowns = Pushdowns::default().with_filters(Some(daft_dsl::lit(true)));
        let reason = parquet_count_pushdown_unsupported_reason(&CountMode::All, &pushdowns, false);
        assert!(reason.is_some_and(|r| r.contains("filter")));
    }

    #[test]
    fn limit_errors() {
        let pushdowns = Pushdowns::default().with_limit(Some(10));
        let reason = parquet_count_pushdown_unsupported_reason(&CountMode::All, &pushdowns, false);
        assert!(reason.is_some_and(|r| r.contains("limit")));
    }

    #[test]
    fn delete_rows_error() {
        let reason =
            parquet_count_pushdown_unsupported_reason(&CountMode::All, &Pushdowns::default(), true);
        assert!(reason.is_some_and(|r| r.contains("delete rows")));
    }
}
