use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_metrics::{Meter, QueryID, StatSnapshot};
use common_partitioning::PartitionRef;
use common_runtime::JoinSet;
use daft_dsl::expr::{BoundColumn, Expr, bound_expr::BoundExpr};
use daft_local_plan::{ExecutionStats, Input, SourceId};
use daft_logical_plan::InMemoryInfo;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::{
    dtype::DataType,
    field::Field,
    schema::{Schema, SchemaRef},
};
use futures::StreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, FilterNode, InMemorySourceNode, ScanSourceNode, SortNode,
    },
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::{
        local_worker::LocalSwordfishWorkerManager,
        scheduler::spawn_scheduler_actor,
        task::{SwordfishTask, Task},
    },
    statistics::{StatisticsManager, TaskEvent},
};

/// Create a simple test schema with one Int64 column named "x".
fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("x", DataType::Int64)]))
}

/// Create a MicroPartition with a single "x" column of Int64 values.
fn make_partition(values: &[i64]) -> Arc<MicroPartition> {
    use daft_core::series::Series;
    let field: daft_schema::field::FieldRef = Field::new("x", DataType::Int64).into();
    let arrow_array: arrow::array::ArrayRef =
        Arc::new(arrow::array::Int64Array::from(values.to_vec()));
    let series = Series::from_arrow(field, arrow_array).unwrap();
    let rb = RecordBatch::new_with_size(test_schema(), vec![series], values.len()).unwrap();
    Arc::new(MicroPartition::new_loaded(
        test_schema(),
        Arc::new(vec![rb]),
        None,
    ))
}

/// Helper: Build a PlanConfig for testing.
fn test_plan_config() -> PlanConfig {
    PlanConfig::new(
        0, // query_idx
        QueryID::from("test-query"),
        Arc::new(DaftExecutionConfig::default()),
    )
}

/// Create a BoundExpr for column "x" at index 0.
fn bound_col_x() -> BoundExpr {
    let field = Field::new("x", DataType::Int64);
    #[allow(deprecated)]
    let col = BoundColumn {
        index: 0,
        field: field.into(),
    };
    let expr = Arc::new(Expr::Column(daft_dsl::expr::Column::Bound(col)));
    BoundExpr::new_unchecked(expr)
}

/// Create a BoundExpr for the predicate `x > 2`.
fn predicate_x_gt_2() -> BoundExpr {
    let col_expr: daft_dsl::ExprRef = {
        let field = Field::new("x", DataType::Int64);
        #[allow(deprecated)]
        let col = BoundColumn {
            index: 0,
            field: field.into(),
        };
        Arc::new(Expr::Column(daft_dsl::expr::Column::Bound(col)))
    };
    let lit_expr = daft_dsl::lit(2i64);
    let gt_expr = col_expr.gt(lit_expr);
    BoundExpr::new_unchecked(gt_expr)
}

/// Helper: Build an InMemorySource → Sort pipeline.
fn build_sort_pipeline(
    partitions: Vec<Arc<MicroPartition>>,
    meter: &Meter,
) -> DistributedPipelineNode {
    let plan_config = test_plan_config();
    let schema = test_schema();
    let cache_key = "test-data".to_string();

    let total_rows: usize = partitions.iter().map(|p| p.len()).sum();
    let total_bytes: usize = partitions.iter().map(|p| p.size_bytes()).sum();
    let num_partitions = partitions.len();

    let partition_refs: Vec<PartitionRef> =
        partitions.into_iter().map(|p| p as PartitionRef).collect();

    let mut psets: HashMap<String, Vec<PartitionRef>> = HashMap::new();
    psets.insert(cache_key.clone(), partition_refs);

    let info = InMemoryInfo {
        source_schema: schema.clone(),
        cache_key,
        cache_entry: None,
        num_partitions,
        size_bytes: total_bytes,
        num_rows: total_rows,
        clustering_spec: None,
        source_stage_id: None,
    };

    let source_node = InMemorySourceNode::new(0, &plan_config, info, Arc::new(psets));
    let source = DistributedPipelineNode::new(Arc::new(source_node), meter);

    let sort_expr = bound_col_x();
    let sort_node = SortNode::new(
        1,
        &plan_config,
        vec![sort_expr],
        vec![false], // descending
        vec![false], // nulls_first
        schema,
        source,
    );

    DistributedPipelineNode::new(Arc::new(sort_node), meter)
}

/// Helper: Build an InMemorySource → Filter pipeline.
fn build_filter_pipeline(
    partitions: Vec<Arc<MicroPartition>>,
    meter: &Meter,
) -> DistributedPipelineNode {
    let plan_config = test_plan_config();
    let schema = test_schema();
    let cache_key = "test-data".to_string();

    let total_rows: usize = partitions.iter().map(|p| p.len()).sum();
    let total_bytes: usize = partitions.iter().map(|p| p.size_bytes()).sum();
    let num_partitions = partitions.len();

    let partition_refs: Vec<PartitionRef> =
        partitions.into_iter().map(|p| p as PartitionRef).collect();

    let mut psets: HashMap<String, Vec<PartitionRef>> = HashMap::new();
    psets.insert(cache_key.clone(), partition_refs);

    let info = InMemoryInfo {
        source_schema: schema.clone(),
        cache_key,
        cache_entry: None,
        num_partitions,
        size_bytes: total_bytes,
        num_rows: total_rows,
        clustering_spec: None,
        source_stage_id: None,
    };

    let source_node = InMemorySourceNode::new(0, &plan_config, info, Arc::new(psets));
    let source = DistributedPipelineNode::new(Arc::new(source_node), meter);

    let predicate = predicate_x_gt_2();
    let filter_node = FilterNode::new(1, &plan_config, predicate, schema, source);

    DistributedPipelineNode::new(Arc::new(filter_node), meter)
}

/// Extract SwordfishTasks from a DistributedPipelineNode by calling produce_tasks().
///
/// Creates a scheduler to satisfy PlanExecutionContext requirements. For non-blocking
/// nodes (Filter, Project, etc.) the scheduler is not used during task production.
/// For blocking nodes (Sort, etc.), the scheduler actually executes intermediate tasks.
///
/// The `stats_manager` is passed to the scheduler so that intermediate task completions
/// can be routed to RuntimeNodeManagers without panicking.
async fn extract_tasks(
    pipeline: &DistributedPipelineNode,
    stats_manager: &crate::statistics::StatisticsManagerRef,
) -> DaftResult<Vec<SwordfishTask>> {
    let worker_manager = Arc::new(LocalSwordfishWorkerManager::single_worker());
    let mut joinset = JoinSet::new();
    let scheduler_handle =
        spawn_scheduler_actor(worker_manager, &mut joinset, stats_manager.clone());

    let mut plan_context = PlanExecutionContext::new(0, scheduler_handle.clone());
    let task_stream = pipeline.clone().produce_tasks(&mut plan_context);

    let task_id_counter = plan_context.task_id_counter();
    let mut tasks = Vec::new();
    tokio::pin!(task_stream);
    while let Some(builder) = task_stream.next().await {
        let submittable = builder.build(0, &task_id_counter);
        tasks.push(submittable.into_task());
    }

    // Clean up: drop the handle and abort the scheduler.
    drop(scheduler_handle);
    joinset.abort_all();

    Ok(tasks)
}

/// Run a SwordfishTask's local plan and return the ExecutionStats.
async fn run_task_locally(task: &SwordfishTask) -> DaftResult<ExecutionStats> {
    let plan = task.plan();
    let config = task.config().clone();
    let psets = task.psets().clone();

    // Convert psets (PartitionRef = Arc<MicroPartition>) to Input::InMemory
    let mut inputs: HashMap<SourceId, Input> = task.inputs().clone();
    for (source_id, partition_refs) in psets {
        let micro_partitions: Vec<Arc<MicroPartition>> = partition_refs
            .into_iter()
            .map(|pr| {
                pr.as_any()
                    .downcast_ref::<MicroPartition>()
                    .map(|mp| Arc::new(mp.clone()))
                    .ok_or_else(|| {
                        common_error::DaftError::InternalError(
                            "PartitionRef is not a MicroPartition".into(),
                        )
                    })
            })
            .collect::<DaftResult<Vec<_>>>()?;
        inputs.insert(source_id, Input::InMemory(micro_partitions));
    }

    let (_partitions, stats) =
        daft_local_execution::testing::execute_local_plan(&plan, config, inputs).await?;
    Ok(stats)
}

/// Feed ExecutionStats from a task into a StatisticsManager.
fn feed_stats_to_manager(
    stats_manager: &StatisticsManager,
    task: &SwordfishTask,
    exec_stats: ExecutionStats,
) -> DaftResult<()> {
    let event = TaskEvent::Completed {
        context: task.task_context(),
        stats: exec_stats,
    };
    stats_manager.handle_event(event)
}

/// End-to-end test helper: build pipeline, extract tasks via produce_tasks(),
/// run each task locally, feed stats to StatisticsManager, export and return.
async fn run_pipeline_and_get_stats(
    pipeline: &DistributedPipelineNode,
    meter: &Meter,
) -> DaftResult<Vec<(Arc<common_metrics::ops::NodeInfo>, StatSnapshot)>> {
    let stats_manager = StatisticsManager::from_pipeline_node(pipeline, vec![], meter)?;

    let tasks = extract_tasks(pipeline, &stats_manager).await?;

    for task in &tasks {
        let exec_stats = run_task_locally(task).await?;
        feed_stats_to_manager(&stats_manager, task, exec_stats)?;
    }

    let exported = stats_manager.export_metrics();
    Ok(exported.nodes)
}

/// Test: Single-partition sort produces correct aggregated stats.
///
/// This exercises the Sort node's FINAL_SORT_PHASE path (single partition optimization).
/// The sort produces two local execution nodes:
/// - an in_memory_scan (Source snapshot)
/// - a sort (Default snapshot)
///
/// SortStats should:
/// - Count duration from all nodes
/// - Count rows_in/rows_out ONLY from the Default snapshot with phase "final-sort"
/// - Ignore the Source snapshot from in_memory_scan
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sort_single_partition_stats() -> DaftResult<()> {
    let meter = Meter::test_scope("test_sort_stats");
    let partition = make_partition(&[5, 3, 1, 4, 2]);
    let pipeline = build_sort_pipeline(vec![partition], &meter);

    let stats = run_pipeline_and_get_stats(&pipeline, &meter).await?;

    // Find the sort node's stats (node_id = 1)
    let sort_stats = stats
        .iter()
        .find(|(info, _)| info.node_origin_id == 1)
        .expect("Sort node stats should be present");

    match &sort_stats.1 {
        StatSnapshot::Default(snapshot) => {
            // Sort doesn't filter rows — rows_in == rows_out == 5
            assert_eq!(
                snapshot.rows_in, 5,
                "Sort should report 5 rows_in from the final sort phase"
            );
            assert_eq!(
                snapshot.rows_out, 5,
                "Sort should report 5 rows_out from the final sort phase"
            );
            assert!(snapshot.cpu_us > 0, "Sort should report non-zero duration");
        }
        other => panic!("Expected Default snapshot for sort, got: {:?}", other),
    }

    // Source node (node_id = 0) should also have stats
    let source_stats = stats
        .iter()
        .find(|(info, _)| info.node_origin_id == 0)
        .expect("Source node stats should be present");

    match &source_stats.1 {
        StatSnapshot::Source(_) => {
            // Source stats are present — validates the SourceStats handler
        }
        other => panic!("Expected Source snapshot for source node, got: {:?}", other),
    }

    Ok(())
}

/// Test: Filter produces correct aggregated stats with selectivity.
///
/// Pipeline: InMemorySource([1,2,3,4,5]) → Filter(x > 2)
/// Expected: rows_in=5, rows_out=3, selectivity=60%
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_filter_stats_aggregation() -> DaftResult<()> {
    let meter = Meter::test_scope("test_filter_stats");
    let partition = make_partition(&[1, 2, 3, 4, 5]);
    let pipeline = build_filter_pipeline(vec![partition], &meter);

    let stats = run_pipeline_and_get_stats(&pipeline, &meter).await?;

    // Find the filter node's stats (node_id = 1)
    let filter_stats = stats
        .iter()
        .find(|(info, _)| info.node_origin_id == 1)
        .expect("Filter node stats should be present");

    match &filter_stats.1 {
        StatSnapshot::Filter(snapshot) => {
            assert_eq!(snapshot.rows_in, 5, "Filter should report 5 rows_in");
            assert_eq!(
                snapshot.rows_out, 3,
                "Filter should report 3 rows_out (values 3, 4, 5 pass x > 2)"
            );
            assert!(
                (snapshot.selectivity - 60.0).abs() < 0.01,
                "Filter selectivity should be 60%, got: {}",
                snapshot.selectivity
            );
            assert!(
                snapshot.cpu_us > 0,
                "Filter should report non-zero duration"
            );
        }
        other => panic!("Expected Filter snapshot for filter node, got: {:?}", other),
    }

    Ok(())
}

/// Test: Filter stats correctly aggregate across multiple partitions (tasks).
///
/// Creates a pipeline with 3 partitions, each producing its own task.
/// Verifies that the StatisticsManager correctly sums stats from all tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_filter_stats_multi_partition() -> DaftResult<()> {
    let meter = Meter::test_scope("test_filter_multi");

    // Create 3 partitions with different data
    let p1 = make_partition(&[1, 2, 3]); // filter x > 2: 1 passes
    let p2 = make_partition(&[4, 5]); // filter x > 2: 2 pass
    let p3 = make_partition(&[1, 1, 1, 1]); // filter x > 2: 0 pass

    let pipeline = build_filter_pipeline(vec![p1, p2, p3], &meter);

    let stats = run_pipeline_and_get_stats(&pipeline, &meter).await?;

    let filter_stats = stats
        .iter()
        .find(|(info, _)| info.node_origin_id == 1)
        .expect("Filter node stats should be present");

    match &filter_stats.1 {
        StatSnapshot::Filter(snapshot) => {
            // Total: 3 + 2 + 4 = 9 rows in, 1 + 2 + 0 = 3 rows out
            assert_eq!(
                snapshot.rows_in, 9,
                "Filter should report 9 total rows_in across 3 partitions"
            );
            assert_eq!(
                snapshot.rows_out, 3,
                "Filter should report 3 total rows_out across 3 partitions"
            );
            let expected_selectivity = (3.0 / 9.0) * 100.0;
            assert!(
                (snapshot.selectivity - expected_selectivity).abs() < 0.01,
                "Filter selectivity should be {:.1}%, got: {:.1}%",
                expected_selectivity,
                snapshot.selectivity
            );
        }
        other => panic!("Expected Filter snapshot, got: {:?}", other),
    }

    Ok(())
}

/// Write a single-column Int64 CSV file with the given values and return the
/// number of bytes on disk. Uses the `x` column from `test_schema()`.
///
/// CSV is used here rather than parquet because daft-io's local parquet read
/// path doesn't increment `bytes_read` (local files skip the `CountingReader`
/// wrapper), while daft-csv's local reader calls `io_stats.mark_bytes_read`
/// on every buffer fill.
fn write_csv_file(path: &std::path::Path, values: &[i64]) -> u64 {
    use std::io::Write;
    let mut file = std::fs::File::create(path).expect("create csv file");
    writeln!(file, "x").expect("write header");
    for v in values {
        writeln!(file, "{v}").expect("write row");
    }
    file.sync_all().expect("sync");
    drop(file);
    std::fs::metadata(path).expect("metadata").len()
}

/// Build a Flotilla pipeline with a `ScanSourceNode` over the given CSV files,
/// one `ScanTaskRef` per file.
fn build_scan_pipeline(
    file_paths: &[std::path::PathBuf],
    meter: &Meter,
) -> DistributedPipelineNode {
    use daft_scan::{
        CsvSourceConfig, FileFormatConfig, Pushdowns, ScanSource, ScanSourceKind, ScanTask,
        ScanTaskRef, SourceConfig, storage_config::StorageConfig,
    };

    let schema = test_schema();
    let csv_cfg = CsvSourceConfig {
        delimiter: None,
        has_headers: true,
        double_quote: true,
        quote: None,
        escape_char: None,
        comment: None,
        allow_variable_columns: false,
        buffer_size: None,
        chunk_size: None,
    };
    let source_config = Arc::new(SourceConfig::File(FileFormatConfig::Csv(csv_cfg)));
    let storage_config = Arc::new(StorageConfig::new_internal(false, None));
    let pushdowns = Pushdowns::default();

    let scan_tasks: Vec<ScanTaskRef> = file_paths
        .iter()
        .map(|path| {
            let size = std::fs::metadata(path).expect("file metadata").len();
            Arc::new(ScanTask::new(
                vec![ScanSource {
                    size_bytes: Some(size),
                    metadata: None,
                    statistics: None,
                    partition_spec: None,
                    kind: ScanSourceKind::File {
                        path: path.to_string_lossy().into_owned(),
                        chunk_spec: None,
                        iceberg_delete_files: None,
                        parquet_metadata: None,
                    },
                }],
                source_config.clone(),
                schema.clone(),
                storage_config.clone(),
                pushdowns.clone(),
                None,
            ))
        })
        .collect();

    let plan_config = test_plan_config();
    let scan_source_node = ScanSourceNode::new(
        0,
        &plan_config,
        pushdowns,
        Arc::new(scan_tasks),
        schema,
    );

    DistributedPipelineNode::new(Arc::new(scan_source_node), meter)
}

/// Repro: Flotilla overreports `bytes_read` when a scan has multiple files.
///
/// Originally observed when reading from S3. The same overreporting reproduces
/// locally with CSV files because daft-csv's local reader calls
/// `io_stats.mark_bytes_read` on every buffer fill (daft-parquet's local path
/// skips the counting reader, so parquet can't reproduce it without S3).
///
/// Root cause (to be fixed): in Flotilla production, `NativeExecutor::run`
/// caches local pipelines by `plan_fingerprint` and reuses a single pipeline
/// across tasks with identical plans. All scan tasks produced by a single
/// `ScanSourceNode` share the same fingerprint, so they share one
/// `SourceNode` — which owns a single `IOStatsRef` shared across per-input_id
/// `SourceStats`. Every `SourceStats::build_snapshot` reads the same atomic
/// counter, so `take_input_snapshot(input_id=N)` for each of N scan tasks
/// returns the cumulative bytes read by *all* tasks so far. When Flotilla's
/// `StatisticsManager` sums those per-task snapshots it reports ~N× the true
/// bytes read.
///
/// This test exercises that exact path via `execute_local_plan_shared`, which
/// mirrors `NativeExecutor::run` by driving multiple scan inputs through one
/// pipeline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_scan_source_bytes_read_multiple_files() -> DaftResult<()> {
    let meter = Meter::test_scope("test_scan_bytes_read_multi_file");

    // Write N distinct CSV files to a fresh temp directory.
    let tmpdir = std::env::temp_dir().join(format!(
        "daft_flotilla_bytes_read_repro_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir_all(&tmpdir).expect("create tmpdir");

    let num_files: usize = 5;
    let rows_per_file: usize = 1_000;
    let mut file_paths = Vec::with_capacity(num_files);
    let mut total_bytes_on_disk: u64 = 0;
    for i in 0..num_files {
        let path = tmpdir.join(format!("file_{i}.csv"));
        let values: Vec<i64> =
            (0..rows_per_file).map(|j| (i * rows_per_file + j) as i64).collect();
        total_bytes_on_disk += write_csv_file(&path, &values);
        file_paths.push(path);
    }

    let pipeline = build_scan_pipeline(&file_paths, &meter);

    let stats_manager = StatisticsManager::from_pipeline_node(&pipeline, vec![], &meter)?;
    let tasks = extract_tasks(&pipeline, &stats_manager).await?;
    assert_eq!(
        tasks.len(),
        num_files,
        "expected one SwordfishTask per file (ScanTaskRef)"
    );

    // All tasks from a single ScanSourceNode share the same plan fingerprint,
    // so in production Flotilla they would share one NativeExecutor pipeline.
    // We simulate that via `execute_local_plan_shared`: one pipeline, N inputs,
    // each on a distinct InputId — matching NativeExecutor's plan-sharing path.
    let first = &tasks[0];
    let plan = first.plan();
    let config = first.config().clone();
    let shared_fingerprint = first.task_context().plan_fingerprint;
    for (i, t) in tasks.iter().enumerate() {
        assert_eq!(
            t.task_context().plan_fingerprint,
            shared_fingerprint,
            "task {i} has a different plan_fingerprint from task 0; shared-pipeline test \
             assumption broken — Flotilla would not share a pipeline across these tasks"
        );
    }

    let batches: Vec<(daft_local_plan::InputId, HashMap<SourceId, Input>)> = tasks
        .iter()
        .enumerate()
        .map(|(i, task)| {
            let inputs = task.inputs().clone();
            let input_id = i as daft_local_plan::InputId;
            (input_id, inputs)
        })
        .collect();

    let per_input_stats =
        daft_local_execution::testing::execute_local_plan_shared(&plan, config, batches)
            .await?;

    let mut per_task_bytes_read: Vec<u64> = Vec::with_capacity(tasks.len());
    for (i, task) in tasks.iter().enumerate() {
        let stats = per_input_stats
            .iter()
            .find(|(iid, _)| *iid as usize == i)
            .map(|(_, s)| s.clone())
            .expect("per-input_id stats should be present");
        let mut task_bytes_read: u64 = 0;
        for (_info, snapshot) in &stats.nodes {
            if let StatSnapshot::Source(s) = snapshot {
                task_bytes_read += s.bytes_read;
            }
        }
        per_task_bytes_read.push(task_bytes_read);
        feed_stats_to_manager(&stats_manager, task, stats)?;
    }

    let swordfish_sum: u64 = per_task_bytes_read.iter().sum();

    let exported = stats_manager.export_metrics();
    let source_stats = exported
        .nodes
        .iter()
        .find(|(info, _)| info.node_origin_id == 0)
        .expect("scan source node stats should be present");

    let flotilla_bytes_read = match &source_stats.1 {
        StatSnapshot::Source(s) => s.bytes_read,
        other => panic!("expected Source snapshot for scan source, got: {:?}", other),
    };

    // Cleanup before any assertion may fail.
    let _ = std::fs::remove_dir_all(&tmpdir);

    eprintln!(
        "total_bytes_on_disk = {total_bytes_on_disk}, \
         per-task bytes_read (as Flotilla sees them) = {per_task_bytes_read:?} \
         (sum = {swordfish_sum}), \
         flotilla aggregated bytes_read = {flotilla_bytes_read}"
    );

    // Flotilla aggregated bytes_read is a sum of per-task snapshots, so by
    // construction it equals `swordfish_sum`. The real assertion is that this
    // sum is close to the actual bytes on disk — not a large multiple of it.
    assert!(
        flotilla_bytes_read <= total_bytes_on_disk * 3,
        "Flotilla bytes_read ({flotilla_bytes_read}) is more than 3x total file size \
         ({total_bytes_on_disk}) — bytes.read is being overreported. Per-task \
         bytes_read = {per_task_bytes_read:?}"
    );

    Ok(())
}

/// Test: Multi-partition sort produces correct phase-filtered aggregated stats.
///
/// With multiple input partitions, the sort goes through all three phases:
/// 1. Sample phase — samples data from each partition
/// 2. Repartition phase — range-repartitions based on sampled boundaries
/// 3. Final sort phase — sorts each repartitioned partition
///
/// SortStats should:
/// - Count duration from ALL phases (sample + repartition + final-sort)
/// - Count rows ONLY from FINAL_SORT_PHASE Default snapshots
/// - Ignore sample/repartition phase rows to avoid double-counting
///
/// This test exercises the REAL interaction: produce_tasks() drives the sort
/// node's execution_loop, the LocalSwordfishWorker executes all intermediate
/// and final tasks, and the StatisticsManager aggregates via the real SortStats.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sort_multi_partition_stats() -> DaftResult<()> {
    let meter = Meter::test_scope("test_sort_multi");

    // Create 3 partitions with different data — total 9 rows.
    let p1 = make_partition(&[5, 3, 1]);
    let p2 = make_partition(&[4, 2, 6]);
    let p3 = make_partition(&[9, 7, 8]);
    let pipeline = build_sort_pipeline(vec![p1, p2, p3], &meter);

    let stats = run_pipeline_and_get_stats(&pipeline, &meter).await?;

    // Find the sort node's stats (node_id = 1).
    let sort_stats = stats
        .iter()
        .find(|(info, _)| info.node_origin_id == 1)
        .expect("Sort node stats should be present");

    match &sort_stats.1 {
        StatSnapshot::Default(snapshot) => {
            // Sort processes all 9 rows through the final sort phase.
            // rows_in and rows_out should equal 9 (sort doesn't drop rows).
            assert_eq!(
                snapshot.rows_in, 9,
                "Multi-partition sort should report 9 rows_in (only from final-sort phase)"
            );
            assert_eq!(
                snapshot.rows_out, 9,
                "Multi-partition sort should report 9 rows_out (only from final-sort phase)"
            );
            // Duration should include contributions from all phases (sample + repartition + final-sort).
            assert!(
                snapshot.cpu_us > 0,
                "Sort should report non-zero duration aggregated from all phases"
            );
        }
        other => panic!("Expected Default snapshot for sort, got: {:?}", other),
    }

    Ok(())
}
