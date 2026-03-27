use std::{borrow::Cow, sync::atomic::Ordering};

use common_metrics::{
    Counter, JOIN_BUILD_ROWS_INSERTED_KEY, JOIN_PROBE_ROWS_IN_KEY, JOIN_PROBE_ROWS_OUT_KEY, Meter,
    StatSnapshot, UNIT_ROWS,
    ops::NodeInfo,
    snapshot::{JoinSnapshot, StatSnapshotImpl as _},
};
use opentelemetry::KeyValue;

use crate::runtime_stats::{RuntimeStats, RuntimeStatsRef};

pub(crate) struct JoinStats {
    duration_us: Counter,
    build_rows_inserted: Counter,
    probe_rows_in: Counter,
    probe_rows_out: Counter,
    node_kv: Vec<KeyValue>,
    right_child_stats: RuntimeStatsRef,
}

impl JoinStats {
    pub(crate) fn new(
        meter: &Meter,
        node_info: &NodeInfo,
        right_child_stats: RuntimeStatsRef,
    ) -> Self {
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
            node_kv,
            right_child_stats,
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
    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        StatSnapshot::Join(JoinSnapshot {
            duration_us: self.duration_us.load(ordering),
            build_rows_inserted: self.build_rows_inserted.load(ordering),
            probe_rows_in: self.probe_rows_in.load(ordering),
            probe_rows_out: self.probe_rows_out.load(ordering),
            estimated_total_probe_rows: self.right_child_stats.build_snapshot(ordering).total(),
        })
    }

    fn add_duration_us(&self, duration_us: u64) {
        self.duration_us.add(duration_us, self.node_kv.as_slice());
    }
}
