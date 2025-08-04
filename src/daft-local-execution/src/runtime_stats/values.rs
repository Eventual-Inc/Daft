use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use common_metrics::{snapshot, Stat, StatSnapshotSend};

// Common statistic names
pub const ROWS_RECEIVED_KEY: &str = "rows received";
pub const ROWS_EMITTED_KEY: &str = "rows emitted";
pub const CPU_US_KEY: &str = "cpu us";

// ----------------------- General Traits for Runtime Stat Collection ----------------------- //

pub trait RuntimeStats: Send + Sync + std::any::Any {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync>;
    /// Create a snapshot of the current statistics.
    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshotSend;
    /// Get a snapshot of the current statistics. Doesn't need to be completely accurate.
    fn snapshot(&self) -> StatSnapshotSend {
        self.build_snapshot(Ordering::Relaxed)
    }
    /// Get a snapshot of the final statistics. Should be accurate.
    fn flush(&self) -> StatSnapshotSend {
        self.build_snapshot(Ordering::SeqCst)
    }

    // Default required properties. TODO: Consider removing?
    fn add_rows_received(&self, rows: u64);
    fn add_rows_emitted(&self, rows: u64);
    fn add_cpu_us(&self, cpu_us: u64);
}

#[derive(Default)]
pub struct DefaultRuntimeStats {
    cpu_us: AtomicU64,
    rows_received: AtomicU64,
    rows_emitted: AtomicU64,
}

impl RuntimeStats for DefaultRuntimeStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshotSend {
        snapshot![
            CPU_US_KEY; Stat::Duration(Duration::from_micros(self.cpu_us.load(ordering))),
            ROWS_RECEIVED_KEY; Stat::Count(self.rows_received.load(ordering)),
            ROWS_EMITTED_KEY; Stat::Count(self.rows_emitted.load(ordering)),
        ]
    }

    fn add_rows_received(&self, rows: u64) {
        self.rows_received.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_rows_emitted(&self, rows: u64) {
        self.rows_emitted.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.fetch_add(cpu_us, Ordering::Relaxed);
    }
}
