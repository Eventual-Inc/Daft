use std::{borrow::Cow, sync::atomic::Ordering};

use common_metrics::{
    Counter, JOIN_BUILD_BYTES_INSERTED_KEY, JOIN_BUILD_ROWS_INSERTED_KEY, JOIN_PROBE_BYTES_IN_KEY,
    JOIN_PROBE_BYTES_OUT_KEY, JOIN_PROBE_ROWS_IN_KEY, JOIN_PROBE_ROWS_OUT_KEY, Meter, StatSnapshot,
    UNIT_BYTES, UNIT_ROWS,
    ops::NodeInfo,
    snapshot::{JoinSnapshot, StatSnapshotImpl as _},
};
use opentelemetry::KeyValue;

use crate::{
    pipeline_node::{PipelineNodeContext, metrics::key_values_from_context},
    statistics::RuntimeStats,
};

pub(crate) struct BasicJoinStats {
    duration_us: Counter,
    build_rows_inserted: Counter,
    probe_rows_in: Counter,
    probe_rows_out: Counter,
    build_bytes_inserted: Counter,
    probe_bytes_in: Counter,
    probe_bytes_out: Counter,
    num_tasks: Counter,
    node_kv: Vec<KeyValue>,
}

impl BasicJoinStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
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
            num_tasks: meter.num_tasks_metric(),
            node_kv,
        }
    }
}

impl RuntimeStats for BasicJoinStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        self.duration_us
            .add(snapshot.duration_us(), self.node_kv.as_slice());
        let StatSnapshot::Join(snapshot) = snapshot else {
            return;
        };

        self.build_rows_inserted
            .add(snapshot.build_rows_inserted, self.node_kv.as_slice());
        self.probe_rows_in
            .add(snapshot.probe_rows_in, self.node_kv.as_slice());
        self.probe_rows_out
            .add(snapshot.probe_rows_out, self.node_kv.as_slice());
        self.build_bytes_inserted
            .add(snapshot.build_bytes_inserted, self.node_kv.as_slice());
        self.probe_bytes_in
            .add(snapshot.probe_bytes_in, self.node_kv.as_slice());
        self.probe_bytes_out
            .add(snapshot.probe_bytes_out, self.node_kv.as_slice());
    }

    fn export_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Join(JoinSnapshot {
            cpu_us: self.duration_us.load(Ordering::SeqCst),
            build_rows_inserted: self.build_rows_inserted.load(Ordering::SeqCst),
            probe_rows_in: self.probe_rows_in.load(Ordering::SeqCst),
            probe_rows_out: self.probe_rows_out.load(Ordering::SeqCst),
            build_bytes_inserted: self.build_bytes_inserted.load(Ordering::SeqCst),
            probe_bytes_in: self.probe_bytes_in.load(Ordering::SeqCst),
            probe_bytes_out: self.probe_bytes_out.load(Ordering::SeqCst),
            num_tasks: self.num_tasks.load(Ordering::SeqCst),
            spill: None,
        })
    }

    fn increment_num_tasks(&self) {
        self.num_tasks.add(1, self.node_kv.as_slice());
    }
}
