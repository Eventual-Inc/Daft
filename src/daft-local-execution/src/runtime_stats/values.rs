use std::sync::atomic::Ordering;

use common_metrics::{
    Counter, MaxGauge, Meter, StatSnapshot, ops::NodeInfo, snapshot::DefaultSnapshot,
};
use opentelemetry::KeyValue;

// ----------------------- General Traits for Runtime Stat Collection ----------------------- //

pub trait RuntimeStats: Send + Sync + std::any::Any {
    fn new(meter: &Meter, node_info: &NodeInfo) -> Self
    where
        Self: Sized;

    /// Create a snapshot of the current statistics.
    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot;
    /// Get a snapshot of the current statistics. Doesn't need to be completely accurate.
    fn snapshot(&self) -> StatSnapshot {
        self.build_snapshot(Ordering::Relaxed)
    }
    /// Get a snapshot of the final statistics. Must be accurate.
    fn flush(&self) -> StatSnapshot {
        self.build_snapshot(Ordering::SeqCst)
    }

    // Default required properties. TODO: Consider removing?
    fn add_rows_in(&self, rows: u64);
    fn add_rows_out(&self, rows: u64);
    fn add_duration_us(&self, duration_us: u64);
    fn add_bytes_in(&self, bytes: u64);
    fn add_bytes_out(&self, bytes: u64);
    /// Record that one more task contributed work to this node's stats.
    /// The unit of "task" is per-operator: intermediate operators fire this
    /// once per processed batch; streaming sinks fire once per dispatched
    /// `op.execute` future that returns; blocking sinks fire once per sink
    /// future plus once more at finalize (N+1 for N batches). Values are
    /// not directly comparable across operator kinds — interpret relative
    /// to the operator's own scale.
    fn increment_num_tasks(&self);

    /// Report the current resident state size of this operator's working set
    /// (e.g. sum of buffered MicroPartition sizes, hash-table contents).
    /// Implementations should track the max of all reported values.
    ///
    /// The default is a no-op so stateless operators (Project, Filter, ...)
    /// can opt out simply by not implementing the method on their custom
    /// `RuntimeStats` impl. Stateful operators (Sort, Aggregate, TopN,
    /// Dedup, ...) should call this whenever their state grows, so the
    /// snapshot reflects the high-water mark.
    fn record_state_bytes(&self, _bytes: u64) {}
}

pub struct DefaultRuntimeStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    bytes_in: Counter,
    bytes_out: Counter,
    num_tasks: Counter,
    peak_state_bytes: MaxGauge,
    /// Whether `record_state_bytes` was ever called. The peak gauge is only
    /// reported in snapshots when this is true so stateless operators don't
    /// publish a misleading `0`.
    peak_state_reported: std::sync::atomic::AtomicBool,
    node_kv: Vec<KeyValue>,
}

impl RuntimeStats for DefaultRuntimeStats {
    fn new(meter: &Meter, node_info: &NodeInfo) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            bytes_in: meter.bytes_in_metric(),
            bytes_out: meter.bytes_out_metric(),
            num_tasks: meter.num_tasks_metric(),
            peak_state_bytes: meter.peak_state_bytes_metric(),
            peak_state_reported: std::sync::atomic::AtomicBool::new(false),
            node_kv,
        }
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        let peak_state_bytes = if self.peak_state_reported.load(ordering) {
            Some(self.peak_state_bytes.load(ordering))
        } else {
            None
        };
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(ordering),
            rows_in: self.rows_in.load(ordering),
            rows_out: self.rows_out.load(ordering),
            bytes_in: self.bytes_in.load(ordering),
            bytes_out: self.bytes_out.load(ordering),
            num_tasks: self.num_tasks.load(ordering),
            peak_state_bytes,
        })
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }

    fn add_duration_us(&self, duration_us: u64) {
        self.duration_us.add(duration_us, self.node_kv.as_slice());
    }

    fn add_bytes_in(&self, bytes: u64) {
        self.bytes_in.add(bytes, self.node_kv.as_slice());
    }

    fn add_bytes_out(&self, bytes: u64) {
        self.bytes_out.add(bytes, self.node_kv.as_slice());
    }

    fn increment_num_tasks(&self) {
        self.num_tasks.add(1, self.node_kv.as_slice());
    }

    fn record_state_bytes(&self, bytes: u64) {
        self.peak_state_bytes.record(bytes, self.node_kv.as_slice());
        self.peak_state_reported
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
