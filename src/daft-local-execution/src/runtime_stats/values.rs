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
    /// Subtract bytes no longer retained by this operator (e.g. on finalize).
    #[allow(dead_code)]
    fn sub_bytes_retained(&self, _bytes: u64) {}
    /// Reset bytes retained to zero (e.g. when all state is consumed on finalize).
    fn reset_bytes_retained(&self) {}
    /// Get the current bytes retained value (used by Phase 2 peak attribution).
    #[allow(dead_code)]
    fn get_bytes_retained(&self) -> u64 {
        0
    }
}

pub struct DefaultRuntimeStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    bytes_retained: AtomicU64,
    peak_bytes_retained: AtomicU64,
    bytes_retained_gauge: Gauge,
    node_kv: Vec<KeyValue>,
}

impl RuntimeStats for DefaultRuntimeStats {
    fn new(meter: &Meter, node_info: &NodeInfo) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            bytes_retained: AtomicU64::new(0),
            peak_bytes_retained: AtomicU64::new(0),
            bytes_retained_gauge: meter.bytes_retained_metric(),
            node_kv,
        }
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(ordering),
            rows_in: self.rows_in.load(ordering),
            rows_out: self.rows_out.load(ordering),
            bytes_retained: self.bytes_retained.load(ordering),
            peak_bytes_retained: self.peak_bytes_retained.load(ordering),
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
        let new_val = self.bytes_retained.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak_bytes_retained
            .fetch_max(new_val, Ordering::Relaxed);
        self.bytes_retained_gauge
            .update(new_val as f64, self.node_kv.as_slice());
    }

    fn sub_bytes_retained(&self, bytes: u64) {
        let new_val = self.bytes_retained.fetch_sub(bytes, Ordering::Relaxed) - bytes;
        self.bytes_retained_gauge
            .update(new_val as f64, self.node_kv.as_slice());
    }

    fn reset_bytes_retained(&self) {
        self.bytes_retained.store(0, Ordering::Relaxed);
        self.bytes_retained_gauge
            .update(0.0, self.node_kv.as_slice());
    }

    fn get_bytes_retained(&self) -> u64 {
        self.bytes_retained.load(Ordering::Relaxed)
    }
}
