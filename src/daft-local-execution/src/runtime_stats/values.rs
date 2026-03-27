use std::sync::atomic::{AtomicU64, Ordering};

use common_metrics::{
    Counter, Gauge, Meter, StatSnapshot, ops::NodeInfo, snapshot::DefaultSnapshot,
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

    /// Add bytes currently retained by this operator (e.g. when a morsel is sinked).
    fn add_bytes_retained(&self, _bytes: u64) {}
    /// Reset bytes retained to zero (e.g. when all state is consumed on finalize).
    fn reset_bytes_retained(&self) {}
}

// ----------------------- Shared Bytes Retained Tracker ----------------------- //

/// Tracks current and peak bytes retained by an operator, backed by atomics and an OTEL gauge.
/// Shared between DefaultRuntimeStats and JoinStats to avoid duplicate implementations.
pub(crate) struct BytesRetainedTracker {
    current: AtomicU64,
    peak: AtomicU64,
    gauge: Gauge,
}

impl BytesRetainedTracker {
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            current: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            gauge: meter.bytes_retained_metric(),
        }
    }

    pub(crate) fn add(&self, bytes: u64, attrs: &[opentelemetry::KeyValue]) {
        let new_val = self.current.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(new_val, Ordering::Relaxed);
        self.gauge.update(new_val as f64, attrs);
    }

    pub(crate) fn reset(&self, attrs: &[opentelemetry::KeyValue]) {
        self.current.store(0, Ordering::Relaxed);
        self.gauge.update(0.0, attrs);
    }

    pub(crate) fn load_current(&self, ordering: Ordering) -> u64 {
        self.current.load(ordering)
    }

    pub(crate) fn load_peak(&self, ordering: Ordering) -> u64 {
        self.peak.load(ordering)
    }
}

// ----------------------- Default Runtime Stats ----------------------- //

pub struct DefaultRuntimeStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    retained: BytesRetainedTracker,
    node_kv: Vec<KeyValue>,
}

impl RuntimeStats for DefaultRuntimeStats {
    fn new(meter: &Meter, node_info: &NodeInfo) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            retained: BytesRetainedTracker::new(meter),
            node_kv,
        }
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(ordering),
            rows_in: self.rows_in.load(ordering),
            rows_out: self.rows_out.load(ordering),
            bytes_retained: self.retained.load_current(ordering),
            peak_bytes_retained: self.retained.load_peak(ordering),
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

    fn add_bytes_retained(&self, bytes: u64) {
        self.retained.add(bytes, self.node_kv.as_slice());
    }

    fn reset_bytes_retained(&self) {
        self.retained.reset(self.node_kv.as_slice());
    }
}
