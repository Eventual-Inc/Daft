use std::sync::atomic::{AtomicU64, Ordering};

use common_metrics::{
    Counter, Meter, SpillReporter, StatSnapshot, ops::NodeInfo,
    snapshot::{DefaultSnapshot, SpillSource},
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
}

pub struct DefaultRuntimeStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    bytes_in: Counter,
    bytes_out: Counter,
    spill: SpillReporter,
    /// Latest reported in-memory buffer size for blocking sinks. The
    /// 200ms aggregation in `StatSnapshot::merge` takes the max across
    /// concurrent inputs, giving a high-water reading per tick.
    in_memory_buffer_bytes: AtomicU64,
    node_kv: Vec<KeyValue>,
}

impl DefaultRuntimeStats {
    /// Access to the spill reporter for operators that spill to local disk
    /// (e.g. flight shuffle). Callers pass this `Arc` down to the I/O layer
    /// so it can record bytes/durations/file events directly.
    pub fn spill(&self) -> &SpillReporter {
        &self.spill
    }

    /// Set the current in-memory buffer size for a blocking sink. Callers
    /// should invoke this after every `push`/`sink` that mutates the buffer.
    pub fn set_in_memory_buffer_bytes(&self, bytes: u64) {
        self.in_memory_buffer_bytes.store(bytes, Ordering::Relaxed);
    }
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
            spill: SpillReporter::new(meter, node_info, SpillSource::Native),
            in_memory_buffer_bytes: AtomicU64::new(0),
            node_kv,
        }
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        let buffer_bytes = self.in_memory_buffer_bytes.load(ordering);
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(ordering),
            rows_in: self.rows_in.load(ordering),
            rows_out: self.rows_out.load(ordering),
            bytes_in: self.bytes_in.load(ordering),
            bytes_out: self.bytes_out.load(ordering),
            spill: self.spill.snapshot(ordering),
            in_memory_buffer_bytes: (buffer_bytes > 0).then_some(buffer_bytes),
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
}

#[cfg(test)]
mod tests {
    use common_metrics::{Meter, ops::NodeInfo, snapshot::SpillSource};

    use super::*;

    fn make_stats() -> DefaultRuntimeStats {
        let meter = Meter::test_scope("spill_observability_test");
        let node_info = NodeInfo::default();
        DefaultRuntimeStats::new(&meter, &node_info)
    }

    #[test]
    fn snapshot_omits_spill_when_untouched() {
        let stats = make_stats();
        let StatSnapshot::Default(snap) = stats.snapshot() else {
            panic!("expected Default snapshot");
        };
        assert!(snap.spill.is_none());
        assert!(snap.in_memory_buffer_bytes.is_none());
    }

    #[test]
    fn snapshot_reports_spill_after_reporter_is_used() {
        let stats = make_stats();
        stats.spill().record_bytes_written(1024);
        stats.spill().record_file_created();

        let StatSnapshot::Default(snap) = stats.snapshot() else {
            panic!("expected Default snapshot");
        };
        let spill = snap.spill.expect("spill metrics should be present");
        assert_eq!(spill.source, SpillSource::Native);
        assert_eq!(spill.bytes_written, 1024);
        assert_eq!(spill.file_count, 1);
        assert_eq!(spill.files_resident, 1);
    }

    #[test]
    fn snapshot_reports_in_memory_buffer_bytes() {
        let stats = make_stats();
        stats.set_in_memory_buffer_bytes(4096);

        let StatSnapshot::Default(snap) = stats.snapshot() else {
            panic!("expected Default snapshot");
        };
        assert_eq!(snap.in_memory_buffer_bytes, Some(4096));
    }

    #[test]
    fn buffer_gauge_reflects_latest_value() {
        let stats = make_stats();
        stats.set_in_memory_buffer_bytes(4096);
        stats.set_in_memory_buffer_bytes(8192);
        stats.set_in_memory_buffer_bytes(1024);

        let StatSnapshot::Default(snap) = stats.snapshot() else {
            panic!("expected Default snapshot");
        };
        assert_eq!(snap.in_memory_buffer_bytes, Some(1024));
    }
}
