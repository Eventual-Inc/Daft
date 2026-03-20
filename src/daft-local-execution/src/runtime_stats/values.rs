use std::sync::{Arc, atomic::Ordering};

use common_metrics::{Counter, Meter, StatSnapshot, ops::NodeInfo, snapshot::DefaultSnapshot};
use opentelemetry::KeyValue;

// ----------------------- General Traits for Runtime Stat Collection ----------------------- //

pub trait RuntimeStats: Send + Sync + std::any::Any {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync>;
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
    fn add_cpu_us(&self, cpu_us: u64);
}

pub struct DefaultRuntimeStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    node_kv: Vec<KeyValue>,
}

impl DefaultRuntimeStats {
    pub fn new(meter: &Meter, node_info: &NodeInfo) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            node_kv,
        }
    }
}

impl RuntimeStats for DefaultRuntimeStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(ordering),
            rows_in: self.rows_in.load(ordering),
            rows_out: self.rows_out.load(ordering),
        })
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.duration_us.add(cpu_us, self.node_kv.as_slice());
    }
}
