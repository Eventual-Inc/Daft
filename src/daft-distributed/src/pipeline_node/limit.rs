use std::{cmp::Ordering, collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use common_metrics::{
    Meter, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::StatSnapshotImpl as _,
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::{PlanStats, StatsState};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{DistributedPipelineNode, MaterializedOutput, PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{NodeID, PipelineNodeConfig, PipelineNodeContext},
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::{
        RuntimeStats,
        stats::{BaseCounters, RuntimeStatsRef},
    },
    utils::channel::{Sender, create_channel},
};

const FIRST_LIMIT_PHASE: &str = "local-limit";
const SECOND_LIMIT_PHASE: &str = "post-limit";

pub struct LimitStats {
    base: BaseCounters,
}

impl LimitStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        Self {
            base: BaseCounters::new(meter, context),
        }
    }
}

impl RuntimeStats for LimitStats {
    fn handle_worker_node_stats(&self, node_info: &NodeInfo, snapshot: &StatSnapshot) {
        self.base.add_duration_us(snapshot.duration_us());
        match snapshot {
            StatSnapshot::Default(snapshot) => {
                if let Some(phase) = &node_info.node_phase {
                    // The first limit is used for pruning, the second limit is for the final output
                    if phase == FIRST_LIMIT_PHASE {
                        self.base.add_rows_in(snapshot.rows_in);
                        self.base.add_bytes_in(snapshot.bytes_in);
                    } else if phase == SECOND_LIMIT_PHASE {
                        self.base.add_rows_out(snapshot.rows_out);
                        self.base.add_bytes_out(snapshot.bytes_out);
                    }
                }
            }
            StatSnapshot::Source(snapshot) => {
                if let Some(phase) = &node_info.node_phase
                    && phase == SECOND_LIMIT_PHASE
                {
                    self.base.add_rows_out(snapshot.rows_out);
                    self.base.add_bytes_out(snapshot.bytes_out);
                }
            }
            _ => {} // Limit don't receive stats from other Swordfish nodes
        }
    }

    fn export_snapshot(&self) -> StatSnapshot {
        self.base.export_default_snapshot()
    }

    fn increment_num_tasks(&self) {
        self.base.increment_num_tasks();
    }
}

/// Keeps track of the remaining skip and take.
///
/// Skip is the number of rows to skip if there is an offset.
/// Take is the number of rows to take for the limit.
struct LimitState {
    remaining_skip: usize,
    remaining_take: usize,
}

impl LimitState {
    fn new(limit: usize, offset: Option<usize>) -> Self {
        Self {
            remaining_skip: offset.unwrap_or(0),
            remaining_take: limit,
        }
    }

    fn remaining_skip(&self) -> usize {
        self.remaining_skip
    }

    fn remaining_take(&self) -> usize {
        self.remaining_take
    }

    fn decrement_skip(&mut self, amount: usize) {
        self.remaining_skip = self.remaining_skip.saturating_sub(amount);
    }

    fn decrement_take(&mut self, amount: usize) {
        self.remaining_take = self.remaining_take.saturating_sub(amount);
    }

    fn is_skip_done(&self) -> bool {
        self.remaining_skip == 0
    }

    fn is_take_done(&self) -> bool {
        self.remaining_take == 0
    }

    fn total_remaining(&self) -> usize {
        self.remaining_skip + self.remaining_take
    }
}

pub(crate) struct LimitNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    limit: usize,
    offset: Option<usize>,
    child: DistributedPipelineNode,
}

impl LimitNode {
    const NODE_NAME: &'static str = "Limit";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        limit: usize,
        offset: Option<usize>,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::Limit,
            NodeCategory::StreamingSink,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            limit,
            offset,
            child,
        }
    }

    fn process_materialized_output(
        self: &Arc<Self>,
        materialized_output: MaterializedOutput,
        limit_state: &mut LimitState,
    ) -> Vec<SwordfishTaskBuilder> {
        let mut downstream_tasks = vec![];
        for next_input in materialized_output.split_into_materialized_outputs() {
            let mut num_rows = next_input.num_rows();

            let skip_num_rows = limit_state.remaining_skip().min(num_rows);
            if !limit_state.is_skip_done() {
                limit_state.decrement_skip(skip_num_rows);
                // all input rows are skipped
                if skip_num_rows >= num_rows {
                    continue;
                }

                num_rows -= skip_num_rows;
            }

            let task = match num_rows.cmp(&limit_state.remaining_take()) {
                Ordering::Less | Ordering::Equal => {
                    limit_state.decrement_take(num_rows);
                    let materialized_outputs = vec![next_input];

                    if skip_num_rows > 0 {
                        let (in_memory_scan, psets) =
                            MaterializedOutput::into_in_memory_scan_with_psets(
                                materialized_outputs,
                                self.config.schema.clone(),
                                self.node_id(),
                            );

                        let plan = LocalPhysicalPlan::limit(
                            in_memory_scan,
                            num_rows as u64,
                            Some(skip_num_rows as u64),
                            StatsState::NotMaterialized,
                            LocalNodeContext::new(Some(self.node_id() as usize))
                                .with_phase(SECOND_LIMIT_PHASE),
                        );
                        SwordfishTaskBuilder::new(plan, self.as_ref(), self.node_id())
                            .with_psets(self.node_id(), psets)
                            .extend_fingerprint(num_rows as u32)
                            .extend_fingerprint(skip_num_rows as u32)
                    } else {
                        // No limit applied, just pass-through
                        let (in_memory_scan, psets) =
                            MaterializedOutput::into_in_memory_scan_with_psets_and_phase(
                                materialized_outputs,
                                self.config.schema.clone(),
                                self.node_id(),
                                SECOND_LIMIT_PHASE,
                            );

                        SwordfishTaskBuilder::new(in_memory_scan, self.as_ref(), self.node_id())
                            .with_psets(self.node_id(), psets)
                    }
                }
                Ordering::Greater => {
                    let remaining = limit_state.remaining_take();
                    let materialized_outputs = vec![next_input];
                    let (in_memory_scan, psets) =
                        MaterializedOutput::into_in_memory_scan_with_psets(
                            materialized_outputs,
                            self.config.schema.clone(),
                            self.node_id(),
                        );
                    let plan = LocalPhysicalPlan::limit(
                        in_memory_scan,
                        remaining as u64,
                        Some(skip_num_rows as u64),
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(self.node_id() as usize))
                            .with_phase(SECOND_LIMIT_PHASE),
                    );
                    let task = SwordfishTaskBuilder::new(plan, self.as_ref(), self.node_id())
                        .with_psets(self.node_id(), psets)
                        .extend_fingerprint(remaining as u32)
                        .extend_fingerprint(skip_num_rows as u32);
                    limit_state.decrement_take(remaining);
                    task
                }
            };
            downstream_tasks.push(task);
            if limit_state.is_take_done() {
                break;
            }
        }
        downstream_tasks
    }

    async fn limit_execution_loop(
        self: Arc<Self>,
        input: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        let node_id = self.node_id();
        let mut limit_state = LimitState::new(self.limit, self.offset);
        let mut input_exhausted = false;

        let mut input = std::pin::pin!(input);

        // Estimate initial concurrency by pulling builders off the input stream and summing
        // their per-builder row estimates (e.g. Parquet row counts) until the cumulative
        // estimate covers `total_remaining`. The pulled builders form the first batch and
        // their count becomes `max_concurrent_tasks`.
        //
        // This avoids the bottleneck of running one task sequentially just to measure output
        // size, and unlike a single-sample estimate (peek the first builder, extrapolate from
        // it), it handles per-builder row-count skew.
        //
        // Stops accumulating on the first builder without an estimate (still queued for
        // submission) — we don't trust a partial sum to extrapolate. Capped at
        // `scantask_max_parallel` to bound buffering and avoid over-launching.
        let mut prefetched: VecDeque<SwordfishTaskBuilder> = VecDeque::new();
        let prefetch_cap = self
            .config
            .execution_config
            .scantask_max_parallel
            .max(1);
        let target = limit_state.total_remaining();
        let mut cumulative_est_rows: usize = 0;
        while cumulative_est_rows < target && prefetched.len() < prefetch_cap {
            let Some(builder) = input.next().await else {
                input_exhausted = true;
                break;
            };
            match builder.estimated_num_rows() {
                Some(est) if est > 0 => {
                    cumulative_est_rows = cumulative_est_rows.saturating_add(est);
                    prefetched.push_back(builder);
                }
                _ => {
                    prefetched.push_back(builder);
                    break;
                }
            }
        }
        let mut max_concurrent_tasks = prefetched.len().max(1);

        // Keep submitting local limit tasks as long as we have remaining limit or we have input
        while !input_exhausted {
            let mut local_limits = VecDeque::new();
            let local_limit_per_task = limit_state.total_remaining();

            // Submit tasks until we have max_concurrent_tasks or we run out of input.
            // Drain the prefetch buffer first, then fall back to the input stream.
            for _ in 0..max_concurrent_tasks {
                let next_builder = if let Some(b) = prefetched.pop_front() {
                    Some(b)
                } else {
                    input.next().await
                };
                if let Some(builder) = next_builder {
                    let builder_with_limit = builder
                        .map_plan(self.as_ref(), move |input| {
                            LocalPhysicalPlan::limit(
                                input,
                                local_limit_per_task as u64,
                                Some(0),
                                StatsState::NotMaterialized,
                                LocalNodeContext::new(Some(node_id as usize))
                                    .with_phase(FIRST_LIMIT_PHASE),
                            )
                        })
                        .extend_fingerprint(local_limit_per_task as u32)
                        .extend_fingerprint(0);
                    let submittable =
                        builder_with_limit.build(self.context.query_idx, &task_id_counter);
                    let future = submittable.submit(&scheduler_handle)?;
                    local_limits.push_back(future);
                } else {
                    input_exhausted = true;
                    break;
                }
            }
            let num_local_limits = local_limits.len();
            let mut total_num_rows: usize = 0;
            for future in local_limits {
                let maybe_result = future.await?;
                if let Some(materialized_output) = maybe_result {
                    total_num_rows += materialized_output.num_rows();
                    // Process the result and get the next tasks
                    let next_tasks =
                        self.process_materialized_output(materialized_output, &mut limit_state);
                    if next_tasks.is_empty() {
                        // If all rows need to be skipped, send an empty scan task to allow downstream tasks to
                        // continue running, such as aggregate tasks
                        let empty_plan = LocalPhysicalPlan::in_memory_scan(
                            self.node_id(),
                            self.config.schema.clone(),
                            0,
                            StatsState::Materialized(PlanStats::empty().into()),
                            LocalNodeContext::new(Some(self.node_id() as usize)),
                        );
                        let empty_scan_builder =
                            SwordfishTaskBuilder::new(empty_plan, self.as_ref(), self.node_id())
                                .with_psets(self.node_id(), vec![]);
                        if result_tx.send(empty_scan_builder).await.is_err() {
                            return Ok(());
                        }
                    } else {
                        // Send the next tasks to the result channel
                        for task in next_tasks {
                            if result_tx.send(task).await.is_err() {
                                return Ok(());
                            }
                        }
                    }

                    if limit_state.is_take_done() {
                        break;
                    }
                }
            }

            // Update max_concurrent_tasks based on actual output
            // Only update if we have remaining limit, and we did get some output
            if limit_state.is_take_done() {
                // Drop the input channel to cancel any input tasks
                break;
            } else if total_num_rows > 0 && num_local_limits > 0 {
                let rows_per_task = total_num_rows.div_ceil(num_local_limits);
                max_concurrent_tasks = limit_state.remaining_take().div_ceil(rows_per_task);
            }
        }

        Ok(())
    }
}

impl PipelineNodeImpl for LimitNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(LimitStats::new(meter, self.context()))
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        match &self.offset {
            Some(o) => vec![format!("Limit: Num Rows = {}, Offset = {}", self.limit, o)],
            None => vec![format!("Limit: {}", self.limit)],
        }
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_stream = self.child.clone().produce_tasks(plan_context);
        let (result_tx, result_rx) = create_channel(1);

        plan_context.spawn(self.limit_execution_loop(
            input_stream,
            result_tx,
            plan_context.scheduler_handle(),
            plan_context.task_id_counter(),
        ));
        TaskBuilderStream::from(result_rx)
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use common_error::DaftResult;
    use common_metrics::{Meter, StatSnapshot};
    use daft_scan::{
        CsvSourceConfig, FileFormatConfig, Pushdowns, ScanSource, ScanSourceKind, ScanTask,
        ScanTaskRef, SourceConfig, storage_config::StorageConfig,
    };

    use super::*;
    use crate::pipeline_node::{
        scan_source::ScanSourceNode,
        test_helpers::{
            build_in_memory_source, make_partition, run_pipeline_and_get_stats, test_plan_config,
            test_schema,
        },
    };

    /// Wrap a child source in a `LimitNode` and return the resulting pipeline.
    fn wrap_with_limit(
        source: DistributedPipelineNode,
        plan_config: &PlanConfig,
        limit: usize,
        meter: &Meter,
    ) -> DistributedPipelineNode {
        let limit_node = LimitNode::new(1, plan_config, limit, None, test_schema(), source);
        DistributedPipelineNode::new(Arc::new(limit_node), meter)
    }

    fn write_csv_file(path: &std::path::Path, values: &[i64]) {
        use std::io::Write;
        let mut file = std::fs::File::create(path).expect("create csv file");
        writeln!(file, "x").expect("write header");
        for v in values {
            writeln!(file, "{v}").expect("write row");
        }
        file.sync_all().expect("sync");
    }

    /// Build a CSV-backed `ScanSourceNode`. The scan tasks carry `size_bytes`,
    /// which is what makes `SwordfishTaskBuilder::estimated_num_rows()` return
    /// `Some(_)` via the `approx_num_rows` size-inflation path.
    fn build_csv_scan_source(
        file_paths: &[PathBuf],
        plan_config: &PlanConfig,
        meter: &Meter,
    ) -> DistributedPipelineNode {
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

        let scan_source_node =
            ScanSourceNode::new(0, plan_config, pushdowns, Arc::new(scan_tasks), schema);
        DistributedPipelineNode::new(Arc::new(scan_source_node), meter)
    }

    fn assert_limit_rows_out(
        stats: &[(Arc<common_metrics::ops::NodeInfo>, StatSnapshot)],
        expected: u64,
    ) {
        let (_, snapshot) = stats
            .iter()
            .find(|(i, _)| i.id == 1)
            .expect("limit node stats");
        match snapshot {
            StatSnapshot::Default(s) => assert_eq!(s.rows_out, expected),
            other => panic!("expected Default snapshot for limit, got: {other:?}"),
        }
    }

    /// Limit on an `InMemorySource`. Verifies the limit is correctly enforced
    /// regardless of which prefetch path the execution loop takes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_limit_without_estimated_rows() -> DaftResult<()> {
        let meter = Meter::test_scope("test_limit_without_estimated_rows");
        let (source, plan_config) = build_in_memory_source(
            0,
            vec![
                make_partition(&[1, 2, 3, 4, 5]),
                make_partition(&[6, 7, 8, 9, 10]),
                make_partition(&[11, 12, 13, 14, 15]),
            ],
            &meter,
        );
        let pipeline = wrap_with_limit(source, &plan_config, 7, &meter);

        let stats = run_pipeline_and_get_stats(&pipeline, &meter).await?;
        assert_limit_rows_out(&stats, 7);
        Ok(())
    }

    /// Limit on a CSV-backed `ScanSource`: scan tasks carry `size_bytes`, so
    /// `estimated_num_rows()` returns `Some(_)` and the execution loop accumulates
    /// estimates across builders. Verifies the limit is correctly enforced.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_limit_with_estimated_rows() -> DaftResult<()> {
        let meter = Meter::test_scope("test_limit_with_estimated_rows");

        let tmpdir = std::env::temp_dir().join(format!(
            "daft_flotilla_limit_estimated_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&tmpdir).expect("create tmpdir");

        let num_files: usize = 3;
        let rows_per_file: usize = 5;
        let mut file_paths = Vec::with_capacity(num_files);
        for i in 0..num_files {
            let path = tmpdir.join(format!("file_{i}.csv"));
            let values: Vec<i64> = (0..rows_per_file)
                .map(|j| (i * rows_per_file + j) as i64)
                .collect();
            write_csv_file(&path, &values);
            file_paths.push(path);
        }

        let plan_config = test_plan_config();
        let source = build_csv_scan_source(&file_paths, &plan_config, &meter);
        let pipeline = wrap_with_limit(source, &plan_config, 7, &meter);

        let stats = run_pipeline_and_get_stats(&pipeline, &meter).await?;

        let _ = std::fs::remove_dir_all(&tmpdir);

        assert_limit_rows_out(&stats, 7);
        Ok(())
    }
}
