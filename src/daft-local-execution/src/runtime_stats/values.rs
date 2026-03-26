use std::sync::{Arc, atomic::Ordering};

use common_metrics::{
    Counter, Meter, StatSnapshot,
    ops::NodeInfo,
    snapshot::{DefaultSnapshot, StatSnapshotImpl as _},
};
use opentelemetry::KeyValue;

// ----------------------- General Traits for Runtime Stat Collection ----------------------- //

pub type RuntimeStatsRef = Arc<dyn RuntimeStats>;
pub trait RuntimeStats: Send + Sync + std::any::Any {
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

    fn add_duration_us(&self, duration_us: u64);
}

pub trait IntermediateRuntimeStats: RuntimeStats {
    fn new(meter: &Meter, node_info: &NodeInfo, child_stats: RuntimeStatsRef) -> Self
    where
        Self: Sized;

    // Default required properties. TODO: Consider removing?
    fn add_rows_in(&self, rows: u64);
    fn add_rows_out(&self, rows: u64);
}

pub struct DefaultRuntimeStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,

    child_stats: RuntimeStatsRef,
    node_kv: Vec<KeyValue>,
}

impl IntermediateRuntimeStats for DefaultRuntimeStats {
    fn new(meter: &Meter, node_info: &NodeInfo, child_stats: RuntimeStatsRef) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            child_stats,
            node_kv,
        }
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }
}

impl RuntimeStats for DefaultRuntimeStats {
    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(ordering),
            rows_in: self.rows_in.load(ordering),
            rows_out: self.rows_out.load(ordering),
            estimated_total_rows: self.child_stats.build_snapshot(ordering).total(),
        })
    }

    fn add_duration_us(&self, duration_us: u64) {
        self.duration_us.add(duration_us, self.node_kv.as_slice());
    }
}
