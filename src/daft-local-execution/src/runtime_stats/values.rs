use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use common_metrics::{Stat, StatSnapshotSend, snapshot};
use opentelemetry::{global, metrics::Counter};

// Common statistic names
pub const ROWS_IN_KEY: &str = "rows in";
pub const ROWS_OUT_KEY: &str = "rows out";
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
    fn add_rows_in(&self, rows: u64);
    fn add_rows_out(&self, rows: u64);
    fn add_cpu_us(&self, cpu_us: u64);
}

pub struct DefaultRuntimeStats {
    cpu_us: AtomicU64,
    rows_in: AtomicU64,
    rows_out: AtomicU64,
    cpu_us_otel: Counter<u64>,
    rows_in_otel: Counter<u64>,
    rows_out_otel: Counter<u64>,
}

impl DefaultRuntimeStats {
    pub fn new(name: &str) -> Self {
        let meter = global::meter("runtime_stats");
        let cpu_us_otel = meter.u64_counter(format!("{name}.cpu_us")).build();
        let rows_in_otel = meter.u64_counter(format!("{name}.rows_in")).build();
        let rows_out_otel = meter.u64_counter(format!("{name}.rows_out")).build();

        Self {
            cpu_us: AtomicU64::new(0),
            rows_in: AtomicU64::new(0),
            rows_out: AtomicU64::new(0),
            cpu_us_otel,
            rows_in_otel,
            rows_out_otel,
        }
    }
}

impl RuntimeStats for DefaultRuntimeStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshotSend {
        snapshot![
            CPU_US_KEY; Stat::Duration(Duration::from_micros(self.cpu_us.load(ordering))),
            ROWS_IN_KEY; Stat::Count(self.rows_in.load(ordering)),
            ROWS_OUT_KEY; Stat::Count(self.rows_out.load(ordering)),
        ]
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.fetch_add(rows, Ordering::Relaxed);
        self.rows_in_otel.add(rows, &[]);
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.fetch_add(rows, Ordering::Relaxed);
        self.rows_out_otel.add(rows, &[]);
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.fetch_add(cpu_us, Ordering::Relaxed);
        self.cpu_us_otel.add(cpu_us, &[]);
    }
}
