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

                for (node_info, snapshot) in &stats.nodes {
                    // Local nodes are associated to this node through the node_origin_id
                    if self.node_info.node_origin_id == node_info.node_origin_id {
                        self.runtime_stats
                            .handle_worker_node_stats(node_info, snapshot);
                    }
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
/// up to the distributed pipeline node. Used by any distributed-side rollup that
/// wants the `spill.*` metrics and `bytes.in_memory_buffer` to appear on its
/// exported snapshot.
pub(crate) struct SpillRollupCounters {
    spill_bytes_written: Counter,
    spill_bytes_read: Counter,
    spill_file_count: Counter,
    // Gauge-like: peak in-memory buffer observed across any worker task.
    // Atomic is owned by us; we take `fetch_max` rather than sum, since buffer
    // size is instantaneous, not cumulative.
    max_in_memory_buffer_bytes: AtomicU64,
    // Sum of resident-files counts reported from workers. Each task reports its
    // own still-resident count at task end; summed gives cluster-wide residency.
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
    /// them into the rollup counters.
    pub fn absorb(&self, snapshot: &StatSnapshot) {
        if let Some(spill) = snapshot.spill_metrics() {
            self.spill_bytes_written
                .add(spill.bytes_written, self.node_kv.as_slice());
            self.spill_bytes_read
                .add(spill.bytes_read, self.node_kv.as_slice());
            self.spill_file_count
                .add(spill.file_count, self.node_kv.as_slice());
            // Task-final resident count; summing gives total outstanding files
            // across all tasks that contributed to this distributed node.
            self.spill_files_resident
                .fetch_add(spill.files_resident, Ordering::Relaxed);
        }
        if let Some(buf) = snapshot.in_memory_buffer_bytes() {
            self.max_in_memory_buffer_bytes
                .fetch_max(buf, Ordering::Relaxed);
        }
    }

    /// Produce a `SpillSnapshot` from the accumulated counters, or `None` if
    /// nothing has been recorded.
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
            spill: SpillRollupCounters::new(meter, node_kv.clone()),
            node_kv,
        }
    }

    /// Fold the spill + buffer fields from an incoming worker snapshot into
    /// the distributed-node rollup. Callers should invoke this alongside the
    /// existing `add_*` row/byte methods.
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

    pub fn export_default_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
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
        // Spill/buffer rollup is variant-agnostic (uses trait methods).
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
            // the variant-agnostic spill rollup above (spill.bytes.read).
            StatSnapshot::Source(snapshot) => {
                self.base.add_rows_out(snapshot.rows_in);
                self.base.add_bytes_out(snapshot.bytes_in);
            }
            _ => {}
        }
    }

    fn export_snapshot(&self) -> StatSnapshot {
        self.base.export_default_snapshot()
    }
}
