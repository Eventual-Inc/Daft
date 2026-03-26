use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayAs, DisplayLevel, tree::TreeDisplay};
use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use common_runtime::{JoinSet, combine_stream, get_compute_pool_num_threads, get_io_runtime};
use daft_core::prelude::{Int64Array, SchemaRef, Utf8Array};
use daft_io::IOStatsRef;
use daft_local_plan::InputId;
use daft_micropartition::MicroPartition;
use daft_parquet::read::{ParquetSchemaInferenceOptions, read_parquet_bulk_async};
use daft_scan::{FileFormatConfig, Pushdowns, ScanTask, ScanTaskRef, SourceConfig};
use futures::{FutureExt, Stream, StreamExt};
use tracing::instrument;

use crate::{
    channel::{Receiver, Sender, UnboundedReceiver, create_channel, create_unbounded_channel},
    pipeline::NodeName,
    sources::{
        scan_task_reader,
        source::{Source, SourceStats, SourceStream},
    },
};

pub struct ScanTaskSource {
    receiver: UnboundedReceiver<(InputId, Vec<ScanTaskRef>)>,
    source_config: Option<Arc<SourceConfig>>,
    pushdowns: Pushdowns,
    schema: SchemaRef,
    num_parallel_tasks: usize,
}

impl ScanTaskSource {
    pub fn new(
        receiver: UnboundedReceiver<(InputId, Vec<ScanTaskRef>)>,
        source_config: Option<Arc<SourceConfig>>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        cfg: &DaftExecutionConfig,
    ) -> Self {
        let num_cpus = get_compute_pool_num_threads();
        let num_parallel_tasks = if cfg.scantask_max_parallel > 0 {
            cfg.scantask_max_parallel
        } else {
            num_cpus
        };
        Self {
            receiver,
            source_config,
            pushdowns,
            schema,
            num_parallel_tasks,
        }
    }

    fn spawn_scan_task_processor(
        num_parallel_tasks: usize,
        mut receiver: UnboundedReceiver<(InputId, Vec<ScanTaskRef>)>,
        output_sender: Sender<MicroPartition>,
        io_stats: IOStatsRef,
        chunk_size: usize,
        schema: SchemaRef,
        maintain_order: bool,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let io_runtime = get_io_runtime(true);

        // When maintain_order is true, spawn flattener outside so it drains stream outputs in order.
        let mut flattener_state = if maintain_order {
            let (agg_tx, agg_rx) = create_unbounded_channel::<Receiver<MicroPartition>>();
            let flattener_handle = io_runtime.spawn(run_order_preserving_flattener(
                agg_rx,
                output_sender.clone(),
            ));
            Some((agg_tx, flattener_handle))
        } else {
            None
        };

        io_runtime.spawn(async move {
            let mut task_set = JoinSet::new();
            let mut pending_tasks = VecDeque::new();
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !pending_tasks.is_empty() || !task_set.is_empty() {
                while task_set.len() < num_parallel_tasks && !pending_tasks.is_empty() {
                    let (scan_task, delete_map, input_id) =
                        pending_tasks.pop_front().expect("Pending tasks should not be empty");
                    let sender = match &flattener_state {
                        Some((agg_tx, _)) => {
                            let (stream_tx, stream_rx) = create_channel(1);
                            let _ = agg_tx.send(stream_rx);
                            stream_tx
                        }
                        None => output_sender.clone(),
                    };
                    task_set.spawn(forward_scan_task_stream(
                        scan_task,
                        io_stats.clone(),
                        delete_map,
                        maintain_order,
                        chunk_size,
                        sender,
                        input_id,
                    ));
                }

                tokio::select! {
                    recv_result = receiver.recv(), if !receiver_exhausted => {
                        match recv_result {
                            Some((_input_id, scan_tasks_batch)) if scan_tasks_batch.is_empty() => {
                                let empty = MicroPartition::empty(Some(schema.clone()));
                                match &flattener_state {
                                    Some((agg_tx, _)) => {
                                        let (tx, rx) = create_channel(1);
                                        let _ = tx.send(empty).await;
                                        drop(tx);
                                        let _ = agg_tx.send(rx);
                                    }
                                    None => {
                                        if output_sender.send(empty).await.is_err() {
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some((input_id, scan_tasks_batch)) => {
                                let delete_map =
                                    get_delete_map(&scan_tasks_batch).await?.map(Arc::new);

                                let split_tasks: Vec<Arc<ScanTask>> = scan_tasks_batch
                                    .into_iter()
                                    .flat_map(|scan_task| scan_task.split())
                                    .collect();

                                for scan_task in split_tasks {
                                    pending_tasks.push_back((scan_task, delete_map.clone(), input_id));
                                }
                            }
                            None => {
                                receiver_exhausted = true;
                            }
                        }
                    }
                    Some(join_result) = task_set.join_next(), if !task_set.is_empty() => {
                        match join_result {
                            Ok(Ok(_)) => {}
                            Ok(Err(e)) => {
                                let _ = flattener_state.take();
                                return Err(e.into());
                            }
                            Err(e) => {
                                let _ = flattener_state.take();
                                return Err(e.into());
                            }
                        }
                    }
                }
            }
            debug_assert!(pending_tasks.is_empty(), "Pending tasks should be empty");
            debug_assert!(task_set.is_empty(), "Task set should be empty");
            debug_assert!(receiver_exhausted, "Receiver should be exhausted");

            if let Some((agg_tx, flattener_handle)) = flattener_state {
                drop(agg_tx);
                flattener_handle
                    .await
                    .map_err::<DaftError, _>(|e| e.into())?;
            }

            Ok(())
        })
    }
}

impl Source for ScanTaskSource {
    #[instrument(name = "ScanTaskSource::get_data", level = "info", skip_all)]
    fn get_data(
        self: Box<Self>,
        maintain_order: bool,
        runtime_stats: Arc<SourceStats>,
        chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let (output_sender, output_receiver) = create_channel::<MicroPartition>(1);
        let input_receiver = self.receiver;
        let num_parallel_tasks = self.num_parallel_tasks;

        let io_stats = runtime_stats.io_stats.clone();
        let processor_task = Self::spawn_scan_task_processor(
            num_parallel_tasks,
            input_receiver,
            output_sender,
            io_stats,
            chunk_size,
            self.schema.clone(),
            maintain_order,
        );
        let result_stream = output_receiver.into_stream().map(Ok);
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }

    fn name(&self) -> NodeName {
        if let Some(source_config) = &self.source_config {
            match source_config.as_ref() {
                SourceConfig::File(ffc) => match ffc {
                    FileFormatConfig::Parquet(_) => "Read Parquet".into(),
                    FileFormatConfig::Csv(_) => "Read CSV".into(),
                    FileFormatConfig::Json(_) => "Read JSON".into(),
                    FileFormatConfig::Warc(_) => "Read WARC".into(),
                    FileFormatConfig::Text(_) => "Read Text".into(),
                },
                #[cfg(feature = "python")]
                SourceConfig::Database(_) => "Read Database".into(),
                #[cfg(feature = "python")]
                SourceConfig::PythonFunction { source_name, .. } => {
                    if let Some(source_name) = source_name {
                        format!("Read {source_name} (Python)").into()
                    } else {
                        "Read Python".into()
                    }
                }
            }
        } else {
            "Empty (Scan Task)".into()
        }
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

    fn schema(&self) -> &SchemaRef {
        &self.schema
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
                            pos.expect("pos should not be null in iceberg delete files"),
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

/// Drains a "receiver of receivers" in order, forwarding each inner stream's
/// micropartitions to `output_sender`. Used when `maintain_order` is true.
async fn run_order_preserving_flattener(
    mut agg_rx: UnboundedReceiver<Receiver<MicroPartition>>,
    output_sender: Sender<MicroPartition>,
) {
    while let Some(mut inner_rx) = agg_rx.recv().await {
        while let Some(mp) = inner_rx.recv().await {
            if output_sender.send(mp).await.is_err() {
                return;
            }
        }
    }
}

async fn forward_scan_task_stream(
    scan_task: Arc<ScanTask>,
    io_stats: IOStatsRef,
    delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
    maintain_order: bool,
    chunk_size: usize,
    sender: Sender<MicroPartition>,
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
        let empty = MicroPartition::empty(Some(schema));
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
) -> DaftResult<impl Stream<Item = DaftResult<MicroPartition>> + Send> {
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
        return Err(DaftError::TypeError(
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

    // TODO(rchowell): remove the scan_task_reader module in the near future with a more general DataSource (TableProvider-like) trait.
    let table_stream = scan_task_reader::read_scan_task(
        &scan_task,
        url,
        file_column_names,
        io_client,
        io_stats,
        delete_map,
        maintain_order,
        chunk_size,
    )
    .await?;

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

        let mp = MicroPartition::new_loaded(
            scan_task.materialized_schema(),
            Arc::new(vec![casted_table]),
            stats,
        );
        Ok(mp)
    }))
}
