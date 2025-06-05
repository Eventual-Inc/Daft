use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
};

use async_trait::async_trait;
use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayAs, DisplayLevel};
use common_error::{DaftError, DaftResult};
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_runtime::{combine_stream, get_compute_pool_num_threads, get_io_runtime};
use common_scan_info::{Pushdowns, ScanTaskLike};
use daft_core::prelude::{AsArrow, Int64Array, SchemaRef, Utf8Array};
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_io::IOStatsRef;
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_micropartition::MicroPartition;
use daft_parquet::read::{read_parquet_bulk_async, ParquetSchemaInferenceOptions};
use daft_scan::{ChunkSpec, ScanTask};
use daft_warc::WarcConvertOptions;
use futures::{FutureExt, Stream, StreamExt};
use snafu::ResultExt;
use tracing::instrument;

use crate::{
    channel::{create_channel, Sender},
    sources::source::{Source, SourceStream},
    TaskSet,
};

pub struct ScanTaskSource {
    scan_tasks: Vec<Arc<ScanTask>>,
    num_parallel_tasks: usize,
    schema: SchemaRef,
}

impl ScanTaskSource {
    const MAX_PARALLEL_SCAN_TASKS: usize = 8;

    pub fn new(
        scan_tasks: Vec<Arc<ScanTask>>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        cfg: &DaftExecutionConfig,
    ) -> Self {
        // Determine the number of parallel tasks to run based on available CPU cores and row limits
        let mut num_parallel_tasks = match pushdowns.limit {
            // If we have a row limit, we need to calculate how many parallel tasks we can run
            // without exceeding the limit
            Some(limit) => {
                let mut count = 0;
                let mut remaining_rows = limit as f64;

                // Only examine tasks up to the number of available CPU cores
                for scan_task in scan_tasks.iter().take(Self::MAX_PARALLEL_SCAN_TASKS) {
                    match scan_task.approx_num_rows(Some(cfg)) {
                        // If we can estimate the number of rows for this task
                        Some(estimated_rows) => {
                            remaining_rows -= estimated_rows;
                            count += 1;

                            // Stop adding tasks if we would exceed the row limit
                            if remaining_rows <= 0.0 {
                                break;
                            }
                        }
                        // If we can't estimate rows, conservatively include the task
                        // This ensures we don't underutilize available resources
                        None => count += 1,
                    }
                }
                count
            }
            // If there's no row limit, use all available CPU cores
            None => Self::MAX_PARALLEL_SCAN_TASKS,
        };
        num_parallel_tasks = num_parallel_tasks.min(scan_tasks.len());
        Self {
            scan_tasks,
            num_parallel_tasks,
            schema,
        }
    }

    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }

    /// Spawns the background task that processes scan tasks with limited parallelism
    fn spawn_scan_task_processor(
        &self,
        senders: Vec<Sender<Arc<MicroPartition>>>,
        io_stats: IOStatsRef,
        delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
        maintain_order: bool,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let io_runtime = get_io_runtime(true);
        let scan_tasks = self.scan_tasks.clone();
        let num_parallel_tasks = self.num_parallel_tasks;

        io_runtime.spawn(async move {
            let mut task_set = TaskSet::new();
            let mut scan_task_and_sender_iter = scan_tasks.into_iter().zip(senders.into_iter());

            // Start initial batch of parallel tasks
            for _ in 0..num_parallel_tasks {
                if let Some((scan_task, sender)) = scan_task_and_sender_iter.next() {
                    task_set.spawn(forward_scan_task_stream(
                        scan_task,
                        io_stats.clone(),
                        delete_map.clone(),
                        maintain_order,
                        sender,
                    ));
                }
            }

            // Process remaining tasks as previous ones complete
            while let Some(result) = task_set.join_next().await {
                result??;
                if let Some((scan_task, sender)) = scan_task_and_sender_iter.next() {
                    task_set.spawn(forward_scan_task_stream(
                        scan_task,
                        io_stats.clone(),
                        delete_map.clone(),
                        maintain_order,
                        sender,
                    ));
                }
            }

            Ok(())
        })
    }
}

#[async_trait]
impl Source for ScanTaskSource {
    #[instrument(name = "ScanTaskSource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        maintain_order: bool,
        io_stats: IOStatsRef,
    ) -> DaftResult<SourceStream<'static>> {
        // Get the delete map for the scan tasks, if any
        let delete_map = get_delete_map(&self.scan_tasks).await?.map(Arc::new);

        // Create channels for the scan tasks
        let (senders, receivers) = match maintain_order {
            // If we need to maintain order, we need to create a channel for each scan task
            true => (0..self.scan_tasks.len())
                .map(|_| create_channel::<Arc<MicroPartition>>(0))
                .unzip(),
            // If we don't need to maintain order, we can use a single channel for all scan tasks
            false => {
                let (tx, rx) = create_channel(0);
                (vec![tx; self.scan_tasks.len()], vec![rx])
            }
        };

        // Spawn the scan task processor
        let task = self.spawn_scan_task_processor(senders, io_stats, delete_map, maintain_order);

        // Flatten the receivers into a stream
        let result_stream = flatten_receivers_into_stream(receivers, task.map(|x| x?));

        Ok(Box::pin(result_stream))
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

impl TreeDisplay for ScanTaskSource {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        fn base_display(scan: &ScanTaskSource) -> String {
            let num_scan_tasks = scan.scan_tasks.len();
            let total_bytes: usize = scan
                .scan_tasks
                .iter()
                .map(|st| st.size_bytes_on_disk().unwrap_or(0))
                .sum();

            #[allow(unused_mut)]
            let mut s = format!(
                "ScanTaskSource:
Num Scan Tasks = {num_scan_tasks}
Estimated Scan Bytes = {total_bytes}
"
            );
            #[cfg(feature = "python")]
            if let FileFormatConfig::Database(config) =
                scan.scan_tasks[0].file_format_config().as_ref()
            {
                if num_scan_tasks == 1 {
                    writeln!(s, "SQL Query = {}", &config.sql).unwrap();
                } else {
                    writeln!(s, "SQL Queries = [{},..]", &config.sql).unwrap();
                }
            }
            s
        }
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default => {
                let mut s = base_display(self);
                // We're only going to display the pushdowns and schema for the first scan task.
                let pushdown = self.scan_tasks[0].pushdowns();
                if !pushdown.is_empty() {
                    s.push_str(&pushdown.display_as(DisplayLevel::Compact));
                    s.push('\n');
                }

                let schema = self.scan_tasks[0].schema();
                writeln!(
                    s,
                    "Schema: {{{}}}",
                    schema.display_as(DisplayLevel::Compact)
                )
                .unwrap();

                let tasks = self.scan_tasks.iter();

                writeln!(s, "Scan Tasks: [").unwrap();
                for (i, st) in tasks.enumerate() {
                    if i < 3 || i >= self.scan_tasks.len() - 3 {
                        writeln!(s, "{}", st.as_ref().display_as(DisplayLevel::Compact)).unwrap();
                    } else if i == 3 {
                        writeln!(s, "...").unwrap();
                    }
                }
                writeln!(s, "]").unwrap();

                s
            }
            DisplayLevel::Verbose => {
                let mut s = base_display(self);
                writeln!(s, "Scan Tasks: [").unwrap();

                for st in &self.scan_tasks {
                    writeln!(s, "{}", st.as_ref().display_as(DisplayLevel::Verbose)).unwrap();
                }
                s
            }
        }
    }

    fn get_name(&self) -> String {
        "ScanTaskSource".to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}

// Read all iceberg delete files and return a map of file paths to delete positions
async fn get_delete_map(
    scan_tasks: &[Arc<ScanTask>],
) -> DaftResult<Option<HashMap<String, Vec<i64>>>> {
    let delete_files = scan_tasks
        .iter()
        .flat_map(|st| {
            st.sources
                .iter()
                .filter_map(|source| source.get_iceberg_delete_files())
                .flatten()
                .cloned()
        })
        .collect::<HashSet<_>>();
    if delete_files.is_empty() {
        return Ok(None);
    }

    let (runtime, io_client) = scan_tasks
        .first()
        .unwrap() // Safe to unwrap because we checked that the list is not empty
        .storage_config
        .get_io_client_and_runtime()?;
    let scan_tasks = scan_tasks.to_vec();
    runtime
        .spawn(async move {
            let mut delete_map = scan_tasks
                .iter()
                .flat_map(|st| st.sources.iter().map(|s| s.get_path().to_string()))
                .map(|path| (path, vec![]))
                .collect::<std::collections::HashMap<_, _>>();
            let columns_to_read = Some(vec!["file_path".to_string(), "pos".to_string()]);
            let result = read_parquet_bulk_async(
                delete_files.into_iter().collect(),
                columns_to_read,
                None,
                None,
                None,
                None,
                io_client,
                None,
                get_compute_pool_num_threads(),
                ParquetSchemaInferenceOptions::new(None),
                None,
                None,
                None,
                None,
            )
            .await?;

            for table_result in result {
                let table = table_result?;
                // values in the file_path column are guaranteed by the iceberg spec to match the full URI of the corresponding data file
                // https://iceberg.apache.org/spec/#position-delete-files

                let get_column_by_name = |name| {
                    if let [(idx, _)] = table.schema.get_fields_with_name(name)[..] {
                        Ok(table.get_column(idx))
                    } else {
                        Err(DaftError::SchemaMismatch(format!(
                            "Iceberg delete files must have columns \"file_path\" and \"pos\", found: {}",
                            table.schema
                        )))
                    }
                };

                let file_paths = get_column_by_name("file_path")?.downcast::<Utf8Array>()?;
                let positions = get_column_by_name("pos")?.downcast::<Int64Array>()?;

                for (file, pos) in file_paths
                    .as_arrow()
                    .values_iter()
                    .zip(positions.as_arrow().values_iter())
                {
                    if delete_map.contains_key(file) {
                        delete_map.get_mut(file).unwrap().push(*pos);
                    }
                }
            }
            Ok(Some(delete_map))
        })
        .await?
}

/// Creates the final result stream by flattening receivers and handling task completion
fn flatten_receivers_into_stream(
    receivers: Vec<crate::channel::Receiver<Arc<MicroPartition>>>,
    background_task: impl Future<Output = DaftResult<()>>,
) -> impl Stream<Item = DaftResult<Arc<MicroPartition>>> {
    let flattened_receivers =
        futures::stream::iter(receivers.into_iter().map(|rx| rx.into_stream()))
            .flatten()
            .map(Ok);

    // Handle the background task completion and forward any errors
    combine_stream(Box::pin(flattened_receivers), background_task)
}

async fn forward_scan_task_stream(
    scan_task: Arc<ScanTask>,
    io_stats: IOStatsRef,
    delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
    maintain_order: bool,
    sender: Sender<Arc<MicroPartition>>,
) -> DaftResult<()> {
    let mut stream = stream_scan_task(scan_task, io_stats, delete_map, maintain_order).await?;
    while let Some(result) = stream.next().await {
        if sender.send(result?).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn stream_scan_task(
    scan_task: Arc<ScanTask>,
    io_stats: IOStatsRef,
    delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
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
                delete_rows,
                *chunk_size,
            )
            .await?
        }
        FileFormatConfig::Csv(cfg) => {
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
        #[allow(deprecated)]
        let casted_table = table.cast_to_schema_with_fill(
            scan_task.materialized_schema().as_ref(),
            scan_task
                .partition_spec()
                .as_ref()
                .map(|pspec| pspec.to_fill_map())
                .as_ref(),
        )?;

        let stats = scan_task
            .statistics
            .as_ref()
            .map(|stats| {
                #[allow(deprecated)]
                stats.cast_to_schema(&scan_task.materialized_schema())
            })
            .transpose()?;

        let mp = Arc::new(MicroPartition::new_loaded(
            scan_task.materialized_schema(),
            Arc::new(vec![casted_table]),
            stats,
        ));
        Ok(mp)
    }))
}
