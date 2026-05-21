use std::sync::atomic::Ordering;

use common_metrics::{Counter, Meter, StatSnapshot, ops::NodeInfo, snapshot::DefaultSnapshot};
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

    /// Optional sink-output hooks. Most nodes do not distinguish generic
    /// output from materialized writes, so row/byte write defaults delegate
    /// to generic output counters.
    fn add_rows_written(&self, rows: u64) {
        self.add_rows_out(rows);
    }
    fn add_bytes_written(&self, bytes: u64) {
        self.add_bytes_out(bytes);
    }
    fn add_partitions_written(&self, _partitions: u64) {}
    fn add_finalize_duration_us(&self, _duration_us: u64) {}
}

pub struct DefaultRuntimeStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    bytes_in: Counter,
    bytes_out: Counter,
    num_tasks: Counter,
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
            node_kv,
        }
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(ordering),
            rows_in: self.rows_in.load(ordering),
            rows_out: self.rows_out.load(ordering),
            bytes_in: self.bytes_in.load(ordering),
            bytes_out: self.bytes_out.load(ordering),
            num_tasks: self.num_tasks.load(ordering),
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
}

#[cfg(test)]
mod tests {
    use common_metrics::ops::NodeInfo;

    use super::*;

    // Defaults must delegate add_rows_written → rows_out and add_bytes_written →
    // bytes_out, else non-shuffle BlockingSinks would lose accounting through the
    // framework's *_written hooks.
    #[test]
    fn default_stats_add_rows_and_bytes_written_delegates() {
        let stats = DefaultRuntimeStats::new(
            &Meter::test_scope("default_delegation"),
            &NodeInfo::default(),
        );
        stats.add_rows_written(100);
        stats.add_bytes_written(2048);

        let StatSnapshot::Default(snap) = stats.snapshot() else {
            panic!("expected Default snapshot");
        };
        assert_eq!(snap.rows_out, 100);
        assert_eq!(snap.bytes_out, 2048);
    }

    // add_partitions_written and add_finalize_duration_us are no-ops on the
    // base trait; DefaultSnapshot has no fields for them.
    #[test]
    fn default_stats_partitions_and_finalize_duration_are_noops() {
        let stats =
            DefaultRuntimeStats::new(&Meter::test_scope("default_noops"), &NodeInfo::default());
        stats.add_rows_in(50);
        stats.add_partitions_written(42);
        stats.add_finalize_duration_us(1000);

        let StatSnapshot::Default(snap) = stats.snapshot() else {
            panic!("expected Default snapshot");
        };
        assert_eq!(snap.rows_in, 50);
        assert_eq!(snap.rows_out, 0);
        assert_eq!(snap.bytes_out, 0);
        assert_eq!(snap.cpu_us, 0);
    }
}
