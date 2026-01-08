#![allow(deprecated, reason = "arrow2 migration")]
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayAs, DisplayLevel, tree::TreeDisplay};
use common_error::{DaftError, DaftResult};
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_metrics::ops::NodeType;
use common_runtime::{combine_stream, get_compute_pool_num_threads, get_io_runtime};
use common_scan_info::Pushdowns;
use daft_core::prelude::{Int64Array, SchemaRef, Utf8Array};
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_dsl::{AggExpr, Expr};
use daft_io::{GetRange, IOStatsRef};
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_micropartition::MicroPartition;
use daft_parquet::read::{ParquetSchemaInferenceOptions, read_parquet_bulk_async};
use daft_scan::{ChunkSpec, ScanTask, ScanTaskRef};
use daft_warc::WarcConvertOptions;
use futures::{FutureExt, Stream, StreamExt};
use snafu::ResultExt;
use tracing::instrument;

use crate::{
    TaskSet,
    channel::{Receiver, Sender, create_channel},
    pipeline::NodeName,
    plan_input::InputId,
    sources::source::{Source, SourceStream},
};

pub struct ScanTaskSource {
    receiver: Option<Receiver<(InputId, Vec<ScanTaskRef>)>>,
    pushdowns: Pushdowns,
    schema: SchemaRef,
    num_parallel_tasks: usize,
}

impl ScanTaskSource {
    pub fn new(
        receiver: Receiver<(InputId, Vec<ScanTaskRef>)>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        cfg: &DaftExecutionConfig,
    ) -> Self {
        let num_cpus = get_compute_pool_num_threads();
        // Use scantask_max_parallel from config if set (non-zero), otherwise use num_cpus
        let num_parallel_tasks = if cfg.scantask_max_parallel > 0 {
            cfg.scantask_max_parallel
        } else {
            num_cpus
        };
        Self {
            receiver: Some(receiver),
            pushdowns,
            schema,
            num_parallel_tasks,
        }
    }

    /// Spawns the background task that continuously reads scan tasks from receiver and processes them
    fn spawn_scan_task_processor(
        &self,
        receiver: Receiver<(InputId, Vec<ScanTaskRef>)>,
        output_sender: Sender<Arc<MicroPartition>>,
        io_stats: IOStatsRef,
        chunk_size: usize,
        schema: SchemaRef,
        maintain_order: bool,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let io_runtime = get_io_runtime(true);
        let num_parallel_tasks = self.num_parallel_tasks;
        let io_runtime_clone = io_runtime.clone();

        io_runtime.spawn(async move {
            let (receivers_sender, forwarder_task) = if maintain_order {
                // Channel for receiving ordered task receivers
                // Each forward_scan_task_stream task will send its receiver to this channel
                let (rs, rr) = create_channel::<Receiver<Arc<MicroPartition>>>(num_parallel_tasks * 2);

                // Spawn ordered forwarder task that reads from receivers in order
                let output_sender_for_forwarder = output_sender.clone();
                let ft = io_runtime_clone.spawn(async move {
                    // Read receivers in order and forward their messages
                    while let Some(message) = rr.recv().await {
                        while let Some(partition) = message.recv().await {
                            if output_sender_for_forwarder.send(partition).await.is_err() {
                                return Ok::<(), DaftError>(());
                            }
                        }
                    }
                    Ok(())
                });
                (Some(rs), Some(ft))
            } else {
                (None, None)
            };

            let mut task_set = TaskSet::new();
            // Store pending tasks: (scan_task, delete_map, input_id)
            let mut pending_tasks = VecDeque::new();
            // Track how many scan tasks are pending per input_id
            // When count reaches 0, we send flush for that input_id
            let mut input_id_pending_counts: HashMap<InputId, usize> = HashMap::new();
            let max_parallel = num_parallel_tasks;
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !pending_tasks.is_empty() || task_set.len() > 0 {
                // First, try to spawn from pending_tasks if we have capacity
                while task_set.len() < max_parallel && !pending_tasks.is_empty() {
                    let (scan_task, delete_map, input_id) = pending_tasks.pop_front().unwrap();
                    if maintain_order {
                        // Create channel for this task when spawning
                        let (task_sender, task_receiver) = create_channel::<Arc<MicroPartition>>(1);

                        // Send receiver to ordered forwarder only when spawning (maintains spawn order)
                        if let Some(ref rs) = receivers_sender && rs.send(task_receiver).await.is_err() {
                                return Ok(());

                        }

                        task_set.spawn(forward_scan_task_stream(
                            scan_task,
                            io_stats.clone(),
                            delete_map,
                            maintain_order,
                            chunk_size,
                            task_sender,
                            input_id,
                        ));
                    } else {
                        // All scan tasks share the same output_sender when maintain_order is false
                        task_set.spawn(forward_scan_task_stream(
                            scan_task,
                            io_stats.clone(),
                            delete_map,
                            maintain_order,
                            chunk_size,
                            output_sender.clone(),
                            input_id,
                        ));
                    }
                }

                tokio::select! {
                    // Receive from channel only if we have capacity and receiver is not exhausted
                    recv_result = receiver.recv(), if !receiver_exhausted => {
                        match recv_result {
                            Some((_input_id, scan_tasks_batch)) if scan_tasks_batch.is_empty() => {
                                let empty = Arc::new(MicroPartition::empty(Some(schema.clone())));
                                if output_sender.send(empty).await.is_err() {
                                    return Ok(());
                                }
                            }
                            Some((input_id, scan_tasks_batch)) => {
                                // Compute delete_map for this batch
                                let delete_map = get_delete_map(&scan_tasks_batch).await?.map(Arc::new);

                                // Split all scan tasks for parallelism
                                let split_tasks: Vec<Arc<ScanTask>> = scan_tasks_batch
                                    .into_iter()
                                    .flat_map(|scan_task| scan_task.split())
                                    .collect();

                                // Track how many scan tasks we're processing for this input_id
                                let num_tasks = split_tasks.len();
                                *input_id_pending_counts.entry(input_id).or_insert(0) += num_tasks;

                                // All tasks from this batch share the same delete_map and input_id
                                // Channel will be created when spawning to maintain order
                                for scan_task in split_tasks {
                                    pending_tasks.push_back((
                                        scan_task,
                                        delete_map.clone(),
                                        input_id,
                                    ));
                                }
                            }
                            None => {
                                // Channel is closed, no more tasks will arrive
                                println!("Receiver is exhausted");
                                receiver_exhausted = true;
                            }
                        }
                    }
                    // Wait for a task to complete
                    Some(join_result) = task_set.join_next(), if task_set.len() > 0 => {
                        match join_result {
                            Ok(Ok(completed_input_id)) => {
                                // task_result is DaftResult<InputId>
                                // Decrement the count for this input_id and send flush only when all scan tasks for this input_id are done
                                let count = input_id_pending_counts.get_mut(&completed_input_id).expect("Input id should be present in input_id_pending_counts");
                                *count = count.saturating_sub(1);
                                if *count == 0 {
                                    input_id_pending_counts.remove(&completed_input_id);
                                }
                            }
                            Ok(Err(e)) => {
                                // Error occurred joining the task, return it
                                return Err(e.into());
                            }
                            Err(e) => {
                                // Error occurred joining the task, return it
                                return Err(e.into());
                            }
                        }
                    }
                }
            }
            debug_assert!(pending_tasks.is_empty(), "Pending tasks should be empty");
            debug_assert!(input_id_pending_counts.is_empty(), "Input id pending counts should be empty");
            debug_assert!(task_set.len() == 0, "Task set should be empty");
            debug_assert!(receiver_exhausted, "Receiver should be exhausted");

            // Close the receivers channel to signal the forwarder to exit (only if maintain_order is true)
            if maintain_order {
                if let Some(rs) = receivers_sender {
                    drop(rs);
                }

                // Wait for the forwarder task to finish processing all messages
                if let Some(ft) = forwarder_task {
                    ft.await??;
                }
            }

            Ok(())
        })
    }
}

#[async_trait]
impl Source for ScanTaskSource {
    #[instrument(name = "ScanTaskSource::get_data", level = "info", skip_all)]
    fn get_data(
        &mut self,
        maintain_order: bool,
        io_stats: IOStatsRef,
        chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        // Create output channel for results
        let (output_sender, output_receiver) = create_channel::<Arc<MicroPartition>>(1);
        // Spawn a task that continuously reads from self.receiver and forwards to task_sender
        // Receiver implements Clone, so we can clone it for the spawned task
        let receiver_clone = self.receiver.take().expect("Receiver not found");

        // Spawn the scan task processor that continuously reads from task_receiver
        // Delete maps will be computed per batch in the processor
        let processor_task = self.spawn_scan_task_processor(
            receiver_clone,
            output_sender,
            io_stats,
            chunk_size,
            self.schema.clone(),
            maintain_order,
        );

        // Convert receiver to stream
        let result_stream = output_receiver.into_stream().map(Ok);

        // Combine with processor task to handle errors
        let combined_stream = combine_stream(Box::pin(result_stream), processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }

    fn name(&self) -> NodeName {
        // We can't access scan_tasks here anymore, so use a generic name
        // The actual format will be determined when we receive the tasks
        "ScanTaskSource".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::ScanTask
    }

    fn multiline_display(&self) -> Vec<String> {
        self.display_as(DisplayLevel::Default)
            .lines()
            .map(|s| s.to_string())
            .collect()
    }
}

impl TreeDisplay for ScanTaskSource {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        fn base_display(scan: &ScanTaskSource) -> String {
            format!(
                "ScanTaskSource:
Num Parallel Scan Tasks = {}
",
                scan.num_parallel_tasks
            )
        }
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default => {
                let mut s = base_display(self);
                let pushdown = &self.pushdowns;
                if !pushdown.is_empty() {
                    s.push_str(&pushdown.display_as(DisplayLevel::Compact));
                    s.push('\n');
                }

                writeln!(
                    s,
                    "Schema: {{{}}}",
                    self.schema.display_as(DisplayLevel::Compact)
                )
                .unwrap();

                s
            }
            DisplayLevel::Verbose => {
                let mut s = base_display(self);
                writeln!(
                    s,
                    "Pushdowns: {}",
                    self.pushdowns.display_as(DisplayLevel::Verbose)
                )
                .unwrap();
                s
            }
        }
    }

    fn repr_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id(),
            "type": self.op_type().to_string(),
            "name": self.name(),
        })
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
                    .into_iter()
                    .zip(positions.into_iter())
                    .map(|(file, pos)| {
                        (
                            file.expect("file should not be null in iceberg delete files"),
                            *pos.expect("pos should not be null in iceberg delete files"),
                        )
                    })
                {
                    if delete_map.contains_key(file) {
                        delete_map.get_mut(file).unwrap().push(pos);
                    }
                }
            }
            Ok(Some(delete_map))
        })
        .await?
}

async fn forward_scan_task_stream(
    scan_task: Arc<ScanTask>,
    io_stats: IOStatsRef,
    delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
    maintain_order: bool,
    chunk_size: usize,
    sender: Sender<Arc<MicroPartition>>,
    input_id: InputId,
) -> DaftResult<InputId> {
    let schema = scan_task.materialized_schema();
    let mut stream =
        stream_scan_task(scan_task, io_stats, delete_map, maintain_order, chunk_size).await?;
    let mut has_data = false;
    while let Some(result) = stream.next().await {
        has_data = true;
        let partition = result?;
        if sender.send(partition).await.is_err() {
            break;
        }
    }

    // If no data was emitted, send empty micropartition
    if !has_data {
        let empty = Arc::new(MicroPartition::empty(Some(schema)));
        let _ = sender.send(empty).await;
    }

    Ok(input_id)
}

async fn stream_scan_task(
    scan_task: Arc<ScanTask>,
    io_stats: IOStatsRef,
    delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
    maintain_order: bool,
    chunk_size: usize,
) -> DaftResult<impl Stream<Item = DaftResult<Arc<MicroPartition>>> + Send> {
    let pushdown_columns = scan_task
        .pushdowns
        .columns
        .as_ref()
        .map(|v| v.iter().cloned().collect::<Vec<_>>());

    let file_column_names = match (
        pushdown_columns,
        scan_task.partition_spec().map(|ps| ps.to_fill_map()),
    ) {
        (None, _) => None,
        (Some(columns), None) => Some(columns),

        // If the ScanTask has a partition_spec, we elide reads of partition columns from the file
        (Some(columns), Some(partition_fillmap)) => Some(
            columns
                .into_iter()
                .filter_map(|s| {
                    if partition_fillmap.contains_key(s.as_str()) {
                        None
                    } else {
                        Some(s)
                    }
                })
                .collect::<Vec<_>>(),
        ),
    };

    if scan_task.sources.len() != 1 {
        return Err(common_error::DaftError::TypeError(
            "Reads only supported for single source ScanTasks".to_string(),
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
            chunk_size: chunk_size_from_config,
            ..
        }) => {
            if let Some(aggregation) = &scan_task.pushdowns.aggregation
                && let Expr::Agg(AggExpr::Count(_, _)) = aggregation.as_ref()
            {
                daft_parquet::read::stream_parquet_count_pushdown(
                    url,
                    io_client,
                    Some(io_stats),
                    field_id_mapping.clone(),
                    aggregation,
                )
                .await?
            } else {
                let parquet_chunk_size = chunk_size_from_config.or(Some(chunk_size));
                let inference_options =
                    ParquetSchemaInferenceOptions::new(Some(*coerce_int96_timestamp_unit));

                let delete_rows = delete_map.as_ref().and_then(|m| m.get(url).cloned());
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
                    file_column_names,
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
                    parquet_chunk_size,
                )
                .await?
            }
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
                Some(io_stats.clone()),
                None,
            )
            .await?
        }
        FileFormatConfig::Json(cfg) => {
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
            let table = Python::attach(|py| {
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
        FileFormatConfig::PythonFunction { .. } => {
            let iter = daft_micropartition::python::read_pyfunc_into_table_iter(scan_task.clone())?;
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
