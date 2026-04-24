use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use common_metrics::{
    Counter, Meter, StatSnapshot, TASK_ACTIVE_KEY, TASK_CANCELLED_KEY, TASK_COMPLETED_KEY,
    TASK_FAILED_KEY, UNIT_TASKS, UpDownCounter,
    ops::NodeInfo,
    snapshot::{DefaultSnapshot, SpillSnapshot, SpillSource, StatSnapshotImpl as _},
};
use opentelemetry::KeyValue;

use crate::{
    pipeline_node::{PipelineNodeContext, metrics::key_values_from_context},
    statistics::TaskEvent,
};

pub trait RuntimeStats: Send + Sync + 'static {
    fn handle_worker_node_stats(&self, node_info: &NodeInfo, snapshot: &StatSnapshot);
    /// Returns the accumulated stats.
    fn export_snapshot(&self) -> StatSnapshot;
    /// Record that one more task contributed work to this node's stats.
    /// The unit of "task" is per-operator: in the distributed node manager
    /// this fires once per distributed task whose worker snapshots
    /// attributed work here; in local-execution operators it fires once
    /// per processed batch future. Blocking sinks intentionally count each
    /// per-batch sink future plus the per-input finalize future (N+1 for
    /// N batches), so values are not directly comparable across operator
    /// kinds — interpret relative to the operator's own scale.
    fn increment_num_tasks(&self);
}
pub type RuntimeStatsRef = Arc<dyn RuntimeStats>;

pub struct RuntimeNodeManager {
    node_info: Arc<NodeInfo>,
    pub node_kv: Vec<KeyValue>,
    runtime_stats: RuntimeStatsRef,

    active_tasks: UpDownCounter,
    completed_tasks: Counter,
    failed_tasks: Counter,
    cancelled_tasks: Counter,
}

impl RuntimeNodeManager {
    pub fn new(meter: &Meter, runtime_stats: RuntimeStatsRef, node_info: Arc<NodeInfo>) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            node_info,
            node_kv,
            runtime_stats,
            active_tasks: meter.i64_up_down_counter(TASK_ACTIVE_KEY),
            completed_tasks: meter.u64_counter_with_desc_and_unit(
                TASK_COMPLETED_KEY,
                None,
                Some(UNIT_TASKS.into()),
            ),
            failed_tasks: meter.u64_counter_with_desc_and_unit(
                TASK_FAILED_KEY,
                None,
                Some(UNIT_TASKS.into()),
            ),
            cancelled_tasks: meter.u64_counter_with_desc_and_unit(
                TASK_CANCELLED_KEY,
                None,
                Some(UNIT_TASKS.into()),
            ),
        }
    }

    /// Returns the accumulated stats for this node as (NodeInfo, StatSnapshot) for export to the driver.
    pub fn export_snapshot(&self) -> (Arc<NodeInfo>, StatSnapshot) {
        (self.node_info.clone(), self.runtime_stats.export_snapshot())
    }

    /// The distributed node id (cheap accessor — avoids snapshotting when a
    /// caller only needs to route by id).
    pub fn node_id(&self) -> usize {
        self.node_info.id
    }

    // The four counters below are scheduler-lifecycle state: they track
    // every task whose `context.node_ids` touches this node, incremented
    // by `handle_task_event` regardless of whether the task's workers
    // produced snapshots attributed to this node. That makes them
    // different from `task.count` (the per-operator counter exported via
    // `RuntimeStats::export_snapshot` / `to_stats`), which only counts
    // tasks that actually contributed work here. So at any point
    // `completed_task_count() >= task.count` for the same node, and
    // active + completed + failed + cancelled is the total lifecycle
    // throughput for tasks routed through this node.

    /// Number of tasks currently in flight for this node. Clamped at 0 —
    /// increments and decrements are paired through `handle_task_event`,
    /// so the underlying signed counter should not observably go negative.
    pub fn active_task_count(&self) -> u64 {
        self.active_tasks.load(Ordering::Relaxed).max(0) as u64
    }

    pub fn completed_task_count(&self) -> u64 {
        self.completed_tasks.load(Ordering::Relaxed)
    }

    pub fn failed_task_count(&self) -> u64 {
        self.failed_tasks.load(Ordering::Relaxed)
    }

    pub fn cancelled_task_count(&self) -> u64 {
        self.cancelled_tasks.load(Ordering::Relaxed)
    }

    fn dec_active_tasks(&self) {
        self.active_tasks.add(-1, self.node_kv.as_slice());
    }

    pub fn handle_task_event(&self, event: &TaskEvent) {
        match event {
            TaskEvent::Scheduled { .. } => {
                self.active_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::Completed { stats, .. } => {
                self.dec_active_tasks();
                self.completed_tasks.add(1, self.node_kv.as_slice());

                let mut originated_here = false;
                for (node_info, snapshot) in &stats.nodes {
                    // Local nodes are associated to this node through the node_origin_id
                    if let Some(node_origin_id) = node_info.node_origin_id {
                        if self.node_info.id == node_origin_id {
                            originated_here = true;
                            self.runtime_stats
                                .handle_worker_node_stats(node_info, snapshot);
                        }
                    } else {
                        tracing::debug!(
                            "local node stats missing `origin_node_id`, skipping attribution: {:?}",
                            node_info
                        );
                    }
                }
                if originated_here {
                    self.runtime_stats.increment_num_tasks();
                }
            }
            TaskEvent::Failed { .. } => {
                self.dec_active_tasks();
                self.failed_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::Cancelled { .. } => {
                self.dec_active_tasks();
                self.cancelled_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::Submitted { .. } => (), // We don't track submitted tasks
        }
    }
}

/// Shared spill + in-memory-buffer rollup counters. Rolls worker-side snapshots
/// up to the distributed pipeline node so `spill.*` metrics and
/// `bytes.in_memory_buffer` appear on the exported snapshot alongside the
/// rows/bytes base counters.
pub(crate) struct SpillRollupCounters {
    spill_bytes_written: Counter,
    spill_bytes_read: Counter,
    spill_file_count: Counter,
    /// Peak in-memory buffer observed across any worker task. `fetch_max`
    /// rather than sum — buffer size is instantaneous, not cumulative.
    max_in_memory_buffer_bytes: AtomicU64,
    /// Sum of resident-files counts reported from workers. Each task reports
    /// its own still-resident count at task end; summed gives cluster-wide
    /// residency.
    spill_files_resident: AtomicU64,
    node_kv: Vec<KeyValue>,
}

impl SpillRollupCounters {
    pub fn new(meter: &Meter, node_kv: Vec<KeyValue>) -> Self {
        Self {
            spill_bytes_written: meter.u64_counter(common_metrics::SPILL_BYTES_WRITTEN_STAT_KEY),
            spill_bytes_read: meter.u64_counter(common_metrics::SPILL_BYTES_READ_STAT_KEY),
            spill_file_count: meter.u64_counter(common_metrics::SPILL_FILE_COUNT_STAT_KEY),
            max_in_memory_buffer_bytes: AtomicU64::new(0),
            spill_files_resident: AtomicU64::new(0),
            node_kv,
        }
    }

    /// Pull spill + buffer fields off an incoming worker snapshot and fold
    /// them into the rollup counters. Variant-agnostic — any snapshot whose
    /// `spill_metrics()` / `in_memory_buffer_bytes()` return `Some` contributes.
    pub fn absorb(&self, snapshot: &StatSnapshot) {
        if let Some(spill) = snapshot.spill_metrics() {
            self.spill_bytes_written
                .add(spill.bytes_written, self.node_kv.as_slice());
            self.spill_bytes_read
                .add(spill.bytes_read, self.node_kv.as_slice());
            self.spill_file_count
                .add(spill.file_count, self.node_kv.as_slice());
            self.spill_files_resident
                .fetch_add(spill.files_resident, Ordering::Relaxed);
        }
        if let Some(buf) = snapshot.in_memory_buffer_bytes() {
            self.max_in_memory_buffer_bytes
                .fetch_max(buf, Ordering::Relaxed);
        }
    }

    pub fn export_spill(&self) -> Option<SpillSnapshot> {
        let bytes_written = self.spill_bytes_written.load(Ordering::Relaxed);
        let bytes_read = self.spill_bytes_read.load(Ordering::Relaxed);
        let file_count = self.spill_file_count.load(Ordering::Relaxed);
        let files_resident = self.spill_files_resident.load(Ordering::Relaxed);
        if bytes_written == 0 && bytes_read == 0 && file_count == 0 && files_resident == 0 {
            return None;
        }
        Some(SpillSnapshot {
            source: SpillSource::Native,
            bytes_written,
            bytes_read,
            file_count,
            files_resident,
        })
    }

    pub fn export_buffer(&self) -> Option<u64> {
        let v = self.max_in_memory_buffer_bytes.load(Ordering::Relaxed);
        (v > 0).then_some(v)
    }
}

pub struct BaseCounters {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    bytes_in: Counter,
    bytes_out: Counter,
    num_tasks: Counter,
    spill: SpillRollupCounters,
    node_kv: Vec<KeyValue>,
}

impl BaseCounters {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            bytes_in: meter.bytes_in_metric(),
            bytes_out: meter.bytes_out_metric(),
            num_tasks: meter.num_tasks_metric(),
            spill: SpillRollupCounters::new(meter, node_kv.clone()),
            node_kv,
        }
    }

    /// Fold spill + buffer fields from an incoming worker snapshot into the
    /// distributed-node rollup. Variant-agnostic; callers invoke this
    /// alongside the existing `add_*` row/byte methods.
    pub fn absorb_spill_and_buffer(&self, snapshot: &StatSnapshot) {
        self.spill.absorb(snapshot);
    }

    pub fn add_duration_us(&self, v: u64) {
        self.duration_us.add(v, self.node_kv.as_slice());
    }

    pub fn add_rows_in(&self, v: u64) {
        self.rows_in.add(v, self.node_kv.as_slice());
    }

    pub fn add_rows_out(&self, v: u64) {
        self.rows_out.add(v, self.node_kv.as_slice());
    }

    pub fn add_bytes_in(&self, v: u64) {
        self.bytes_in.add(v, self.node_kv.as_slice());
    }

    pub fn add_bytes_out(&self, v: u64) {
        self.bytes_out.add(v, self.node_kv.as_slice());
    }

    pub fn increment_num_tasks(&self) {
        self.num_tasks.add(1, self.node_kv.as_slice());
    }

    pub fn export_default_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            num_tasks: self.num_tasks.load(Ordering::Relaxed),
            spill: self.spill.export_spill(),
            in_memory_buffer_bytes: self.spill.export_buffer(),
        })
    }
}

pub struct DefaultRuntimeStats {
    base: BaseCounters,
}

impl DefaultRuntimeStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        Self {
            base: BaseCounters::new(meter, context),
        }
    }
}

impl RuntimeStats for DefaultRuntimeStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        self.base.add_duration_us(snapshot.duration_us());
        // Spill + in-memory buffer rollup is variant-agnostic — any snapshot
        // that carries spill or buffer metrics contributes regardless of kind.
        self.base.absorb_spill_and_buffer(snapshot);

        match snapshot {
            StatSnapshot::Default(snapshot) => {
                self.base.add_rows_in(snapshot.rows_in);
                self.base.add_rows_out(snapshot.rows_out);
                self.base.add_bytes_in(snapshot.bytes_in);
                self.base.add_bytes_out(snapshot.bytes_out);
            }
            // ShuffleRead worker tasks emit `Source`-variant snapshots keyed
            // on the Repartition distributed node_id. Absorb their rows_out /
            // bytes_out so they surface on the Repartition card alongside the
            // write-side bytes_in / rows_in from RepartitionSink. `bytes_read`
            // is intentionally dropped here — spilled-read bytes flow through
            // the variant-agnostic spill rollup above as `spill.bytes.read`.
            StatSnapshot::Source(snapshot) => {
                self.base.add_rows_out(snapshot.rows_out);
                self.base.add_bytes_out(snapshot.bytes_out);
            }
            _ => {
                // TODO: Return immediately for now, but ideally should error
            }
        }
    }

    fn export_snapshot(&self) -> StatSnapshot {
        self.base.export_default_snapshot()
    }

    fn increment_num_tasks(&self) {
        self.base.increment_num_tasks();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_metrics::{
        Meter, NodeID,
        ops::{NodeCategory, NodeInfo, NodeType},
    };
    use daft_local_plan::ExecutionStats;

    use super::*;
    use crate::{
        pipeline_node::PipelineNodeContext, scheduling::task::TaskContext, statistics::TaskEvent,
    };

    fn context() -> PipelineNodeContext {
        PipelineNodeContext::new(
            0,
            "test-query".into(),
            7,
            "Mock".into(),
            common_metrics::ops::NodeType::Project,
            common_metrics::ops::NodeCategory::Intermediate,
        )
    }

    fn default_num_tasks(snapshot: &StatSnapshot) -> u64 {
        match snapshot {
            StatSnapshot::Default(s) => s.num_tasks,
            other => panic!("expected Default snapshot, got {other:?}"),
        }
    }

    #[test]
    fn base_counters_num_tasks_round_trips_via_snapshot() {
        let meter = Meter::test_scope("base_counters_num_tasks");
        let base = BaseCounters::new(&meter, &context());
        for _ in 0..7 {
            base.increment_num_tasks();
        }
        assert_eq!(default_num_tasks(&base.export_default_snapshot()), 7);
    }

    fn runtime_node_manager(node_origin_id: usize) -> RuntimeNodeManager {
        let meter = Meter::test_scope("runtime_node_manager_num_tasks");
        let node_info = Arc::new(NodeInfo {
            id: node_origin_id,
            node_origin_id: Some(node_origin_id),
            ..Default::default()
        });
        let runtime_stats = Arc::new(DefaultRuntimeStats::new(&meter, &context()));
        RuntimeNodeManager::new(&meter, runtime_stats, node_info)
    }

    fn completed_event(worker_node_origin_ids: &[usize]) -> TaskEvent {
        let nodes = worker_node_origin_ids
            .iter()
            .map(|&origin| {
                (
                    Arc::new(NodeInfo {
                        node_origin_id: Some(origin),
                        ..Default::default()
                    }),
                    StatSnapshot::Default(DefaultSnapshot::default()),
                )
            })
            .collect();
        TaskEvent::Completed {
            context: TaskContext::default(),
            stats: ExecutionStats::new("q".into(), nodes),
            worker_id: "worker-1".into(),
        }
    }

    #[test]
    fn handle_task_event_increments_num_tasks_on_origin_match() {
        let mgr = runtime_node_manager(42);
        mgr.handle_task_event(&completed_event(&[42]));
        mgr.handle_task_event(&completed_event(&[42, 42]));
        // Two tasks matched; each increments by 1 regardless of how many
        // worker-node snapshots inside share the same origin_node_id.
        let (_, snapshot) = mgr.export_snapshot();
        assert_eq!(default_num_tasks(&snapshot), 2);
    }

    #[test]
    fn handle_task_event_does_not_increment_num_tasks_without_origin_match() {
        let mgr = runtime_node_manager(42);
        mgr.handle_task_event(&completed_event(&[1, 2, 3]));
        let (_, snapshot) = mgr.export_snapshot();
        assert_eq!(default_num_tasks(&snapshot), 0);
    }

    fn scheduled_event() -> TaskEvent {
        TaskEvent::Scheduled {
            context: TaskContext::default(),
        }
    }

    fn failed_event() -> TaskEvent {
        TaskEvent::Failed {
            context: TaskContext::default(),
            reason: "boom".into(),
            worker_id: None,
            retryable: false,
        }
    }

    fn cancelled_event() -> TaskEvent {
        TaskEvent::Cancelled {
            context: TaskContext::default(),
        }
    }

    #[test]
    fn task_count_getters_reflect_task_events() {
        let mgr = runtime_node_manager(42);
        // 3 scheduled; 1 completes, 1 fails, 1 is cancelled — net active 0.
        mgr.handle_task_event(&scheduled_event());
        mgr.handle_task_event(&scheduled_event());
        mgr.handle_task_event(&scheduled_event());
        mgr.handle_task_event(&completed_event(&[42]));
        mgr.handle_task_event(&failed_event());
        mgr.handle_task_event(&cancelled_event());

        assert_eq!(mgr.active_task_count(), 0);
        assert_eq!(mgr.completed_task_count(), 1);
        assert_eq!(mgr.failed_task_count(), 1);
        assert_eq!(mgr.cancelled_task_count(), 1);
    }

    #[test]
    fn active_task_count_clamps_at_zero_if_never_scheduled() {
        // Failure without a prior Scheduled would push the underlying
        // signed counter below zero; the getter must not leak a negative.
        let mgr = runtime_node_manager(42);
        mgr.handle_task_event(&failed_event());
        assert_eq!(mgr.active_task_count(), 0);
        assert_eq!(mgr.failed_task_count(), 1);
    }

    #[test]
    fn test_runtime_stats_origin_node_id() {
        struct Case {
            name: &'static str,
            node_id: usize,
            origin_node_id: usize,
            expected_cpu_us: u64,
            expected_rows_in: u64,
            expected_rows_out: u64,
            expected_bytes_in: u64,
            expected_bytes_out: u64,
        }

        let distributed_node_id: NodeID = 42;

        let cases = [
            Case {
                name: "matching",
                node_id: distributed_node_id,
                origin_node_id: distributed_node_id,
                expected_cpu_us: 100,
                expected_rows_in: 10,
                expected_rows_out: 5,
                expected_bytes_in: 1000,
                expected_bytes_out: 500,
            },
            Case {
                name: "non_matching",
                node_id: distributed_node_id,
                origin_node_id: 99,
                expected_cpu_us: 0,
                expected_rows_in: 0,
                expected_rows_out: 0,
                expected_bytes_in: 0,
                expected_bytes_out: 0,
            },
        ];

        for case in cases {
            let distributed_node_info = Arc::new(NodeInfo {
                id: case.node_id,
                name: "parent".into(),
                node_origin_id: None,
                node_type: NodeType::Filter,
                node_category: NodeCategory::Intermediate,
                node_phase: None,
                context: HashMap::new(),
            });

            let distributed_ctx = PipelineNodeContext::new(
                0,
                "q".into(),
                case.node_id as u32,
                "distributed-filter".into(),
                NodeType::Filter,
                NodeCategory::Intermediate,
            );

            let meter = Meter::test_scope("runtime-stats-test");
            let runtime_stats = Arc::new(DefaultRuntimeStats::new(&meter, &distributed_ctx));
            let manager = RuntimeNodeManager::new(&meter, runtime_stats, distributed_node_info);

            let local_node_info = Arc::new(NodeInfo {
                id: 7,
                node_origin_id: Some(case.origin_node_id),
                ..Default::default()
            });

            let local_snapshot = StatSnapshot::Default(DefaultSnapshot {
                cpu_us: 100,
                rows_in: 10,
                rows_out: 5,
                bytes_in: 1000,
                bytes_out: 500,
                num_tasks: 0,
                spill: None,
                in_memory_buffer_bytes: None,
            });

            let event = TaskEvent::Completed {
                context: TaskContext::new(0, 42, 1, vec![42], 0),
                stats: ExecutionStats::new("".into(), vec![(local_node_info, local_snapshot)]),
                worker_id: "worker-1".into(),
            };

            manager.handle_task_event(&event);

            let (_info, actual) = manager.export_snapshot();
            let StatSnapshot::Default(got) = &actual else {
                panic!("{}: expected StatSnapshot::Default", case.name);
            };
            assert_eq!(got.cpu_us, case.expected_cpu_us, "{}: cpu_us", case.name);
            assert_eq!(got.rows_in, case.expected_rows_in, "{}: rows_in", case.name);
            assert_eq!(
                got.rows_out, case.expected_rows_out,
                "{}: rows_out",
                case.name
            );
            assert_eq!(
                got.bytes_in, case.expected_bytes_in,
                "{}: bytes_in",
                case.name
            );
            assert_eq!(
                got.bytes_out, case.expected_bytes_out,
                "{}: bytes_out",
                case.name
            );
        }
    }
}
