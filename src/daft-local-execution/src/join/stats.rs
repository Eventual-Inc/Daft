use std::sync::{Arc, atomic::Ordering};

use common_metrics::{Counter, Meter, StatSnapshot, ops::NodeInfo, snapshot::JoinSnapshot};
use opentelemetry::KeyValue;

use crate::runtime_stats::RuntimeStats;

pub(crate) struct JoinStats {
    duration_us: Counter,
    build_rows_inserted: Counter,
    probe_rows_in: Counter,
    probe_rows_out: Counter,
    node_kv: Vec<KeyValue>,
}

impl JoinStats {
    pub fn new(meter: &Meter, node_info: &NodeInfo) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            duration_us: meter.duration_us_metric(),
            build_rows_inserted: meter.u64_counter("build rows inserted"),
            probe_rows_in: meter.u64_counter("probe rows in"),
            probe_rows_out: meter.u64_counter("probe rows out"),
            node_kv,
        }
    }

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
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        StatSnapshot::Join(JoinSnapshot {
            cpu_us: self.duration_us.load(ordering),
            build_rows_inserted: self.build_rows_inserted.load(ordering),
            probe_rows_in: self.probe_rows_in.load(ordering),
            probe_rows_out: self.probe_rows_out.load(ordering),
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

    fn add_cpu_us(&self, cpu_us: u64) {
        self.duration_us.add(cpu_us, self.node_kv.as_slice());
    }
}
