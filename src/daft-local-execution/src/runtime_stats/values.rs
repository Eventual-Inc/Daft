use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use common_metrics::{CPU_US_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, Stat, StatSnapshot, snapshot};
use opentelemetry::{KeyValue, global, metrics::Meter};

// ----------------------- Wrappers for Runtime Stat Values ----------------------- //

pub struct Counter {
    value: AtomicU64,
    otel: opentelemetry::metrics::Counter<u64>,
}

impl Counter {
    pub fn new(
        meter: &Meter,
        name: Cow<'static, str>,
        description: Option<Cow<'static, str>>,
    ) -> Self {
        let builder = meter.u64_counter(name);
        let builder = if let Some(description) = description {
            builder.with_description(description)
        } else {
            builder
        };
        Self {
            value: AtomicU64::new(0),
            otel: builder.build(),
        }
    }

    pub fn add(&self, value: u64, key_values: &[KeyValue]) {
        self.value.fetch_add(value, Ordering::Relaxed);
        self.otel.add(value, key_values);
    }

    pub fn load(&self, ordering: Ordering) -> u64 {
        self.value.load(ordering)
    }
}

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
    cpu_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    node_kv: Vec<KeyValue>,
}

impl DefaultRuntimeStats {
    pub fn new(id: usize) -> Self {
        let meter = global::meter("daft.local.node_stats");
        let node_kv = vec![KeyValue::new("node_id", id.to_string())];

        Self {
            cpu_us: Counter::new(&meter, "cpu_us".into(), None),
            rows_in: Counter::new(&meter, "rows_in".into(), None),
            rows_out: Counter::new(&meter, "rows_out".into(), None),
            node_kv,
        }
    }
}

impl RuntimeStats for DefaultRuntimeStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        snapshot![
            CPU_US_KEY; Stat::Duration(Duration::from_micros(self.cpu_us.load(ordering))),
            ROWS_IN_KEY; Stat::Count(self.rows_in.load(ordering)),
            ROWS_OUT_KEY; Stat::Count(self.rows_out.load(ordering)),
        ]
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.add(cpu_us, self.node_kv.as_slice());
    }
}
