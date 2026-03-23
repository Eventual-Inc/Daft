use std::{
    borrow::Cow,
    sync::atomic::{AtomicU64, Ordering},
};

use common_metrics::{
    Counter, Gauge, JOIN_BUILD_ROWS_INSERTED_KEY, JOIN_PROBE_ROWS_IN_KEY, JOIN_PROBE_ROWS_OUT_KEY,
    Meter, StatSnapshot, UNIT_ROWS, ops::NodeInfo, snapshot::JoinSnapshot,
};
use opentelemetry::KeyValue;

use crate::runtime_stats::RuntimeStats;

pub(crate) struct JoinStats {
    duration_us: Counter,
    build_rows_inserted: Counter,
    probe_rows_in: Counter,
    probe_rows_out: Counter,
    bytes_retained: AtomicU64,
    peak_bytes_retained: AtomicU64,
    bytes_retained_gauge: Gauge,
    node_kv: Vec<KeyValue>,
}

impl JoinStats {
    pub(crate) fn add_build_rows_inserted(&self, rows: u64) {
        self.build_rows_inserted.add(rows, self.node_kv.as_slice());
    }

    pub(crate) fn add_probe_rows_in(&self, rows: u64) {
        self.probe_rows_in.add(rows, self.node_kv.as_slice());
    }

    pub(crate) fn add_probe_rows_out(&self, rows: u64) {
        self.probe_rows_out.add(rows, self.node_kv.as_slice());
    }
}

impl RuntimeStats for JoinStats {
    fn new(meter: &Meter, node_info: &NodeInfo) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            duration_us: meter.duration_us_metric(),
            build_rows_inserted: meter.u64_counter_with_desc_and_unit(
                JOIN_BUILD_ROWS_INSERTED_KEY,
                None,
                Some(Cow::Borrowed(UNIT_ROWS)),
            ),
            probe_rows_in: meter.u64_counter_with_desc_and_unit(
                JOIN_PROBE_ROWS_IN_KEY,
                None,
                Some(Cow::Borrowed(UNIT_ROWS)),
            ),
            probe_rows_out: meter.u64_counter_with_desc_and_unit(
                JOIN_PROBE_ROWS_OUT_KEY,
                None,
                Some(Cow::Borrowed(UNIT_ROWS)),
            ),
            bytes_retained: AtomicU64::new(0),
            peak_bytes_retained: AtomicU64::new(0),
            bytes_retained_gauge: meter.bytes_retained_metric(),
            node_kv,
        }
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        StatSnapshot::Join(JoinSnapshot {
            cpu_us: self.duration_us.load(ordering),
            build_rows_inserted: self.build_rows_inserted.load(ordering),
            probe_rows_in: self.probe_rows_in.load(ordering),
            probe_rows_out: self.probe_rows_out.load(ordering),
            bytes_retained: self.bytes_retained.load(ordering),
            peak_bytes_retained: self.peak_bytes_retained.load(ordering),
        })
    }

    // TODO: Remove these properties from RuntimeStats trait
    fn add_rows_in(&self, _rows: u64) {
        unreachable!(
            "Join Nodes shouldn't receive rows. Use add_build_rows_inserted or add_probe_rows_in instead."
        )
    }

    fn add_rows_out(&self, _rows: u64) {
        unreachable!("Join Nodes shouldn't receive rows. Use add_probe_rows_out instead.")
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
