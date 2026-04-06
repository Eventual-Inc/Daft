use std::{borrow::Cow, sync::atomic::Ordering};

use common_metrics::{
    Counter, JOIN_BUILD_BYTES_INSERTED_KEY, JOIN_BUILD_ROWS_INSERTED_KEY, JOIN_PROBE_BYTES_IN_KEY,
    JOIN_PROBE_BYTES_OUT_KEY, JOIN_PROBE_ROWS_IN_KEY, JOIN_PROBE_ROWS_OUT_KEY, Meter, StatSnapshot,
    UNIT_BYTES, UNIT_ROWS, ops::NodeInfo, snapshot::JoinSnapshot,
};
use opentelemetry::KeyValue;

use crate::runtime_stats::RuntimeStats;

pub(crate) struct JoinStats {
    duration_us: Counter,
    build_rows_inserted: Counter,
    probe_rows_in: Counter,
    probe_rows_out: Counter,
    build_bytes_inserted: Counter,
    probe_bytes_in: Counter,
    probe_bytes_out: Counter,
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

    pub(crate) fn add_build_bytes_inserted(&self, bytes: u64) {
        self.build_bytes_inserted
            .add(bytes, self.node_kv.as_slice());
    }

    pub(crate) fn add_probe_bytes_in(&self, bytes: u64) {
        self.probe_bytes_in.add(bytes, self.node_kv.as_slice());
    }

    pub(crate) fn add_probe_bytes_out(&self, bytes: u64) {
        self.probe_bytes_out.add(bytes, self.node_kv.as_slice());
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
            build_bytes_inserted: meter.u64_counter_with_desc_and_unit(
                JOIN_BUILD_BYTES_INSERTED_KEY,
                None,
                Some(Cow::Borrowed(UNIT_BYTES)),
            ),
            probe_bytes_in: meter.u64_counter_with_desc_and_unit(
                JOIN_PROBE_BYTES_IN_KEY,
                None,
                Some(Cow::Borrowed(UNIT_BYTES)),
            ),
            probe_bytes_out: meter.u64_counter_with_desc_and_unit(
                JOIN_PROBE_BYTES_OUT_KEY,
                None,
                Some(Cow::Borrowed(UNIT_BYTES)),
            ),
            node_kv,
        }
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        StatSnapshot::Join(JoinSnapshot {
            cpu_us: self.duration_us.load(ordering),
            build_rows_inserted: self.build_rows_inserted.load(ordering),
            probe_rows_in: self.probe_rows_in.load(ordering),
            probe_rows_out: self.probe_rows_out.load(ordering),
            build_bytes_inserted: self.build_bytes_inserted.load(ordering),
            probe_bytes_in: self.probe_bytes_in.load(ordering),
            probe_bytes_out: self.probe_bytes_out.load(ordering),
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

    fn add_bytes_in(&self, _bytes: u64) {
        unreachable!(
            "Join Nodes shouldn't receive bytes. Use add_build_bytes_inserted or add_probe_bytes_in instead."
        )
    }

    fn add_bytes_out(&self, _bytes: u64) {
        unreachable!("Join Nodes shouldn't receive bytes. Use add_probe_bytes_out instead.")
    }
}
