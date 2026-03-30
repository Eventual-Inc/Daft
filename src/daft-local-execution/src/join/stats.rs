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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::Ordering};

    use common_metrics::{Meter, StatSnapshot, ops::NodeInfo, snapshot::StatSnapshotImpl as _};

    use super::JoinStats;
    use crate::runtime_stats::RuntimeStats;

    fn node_info_from_id(id: usize) -> NodeInfo {
        NodeInfo {
            id,
            ..Default::default()
        }
    }

    /// Mock right-child stats that report a fixed estimated total.
    struct MockRightChildStats {
        estimated_total: u64,
    }
    impl RuntimeStats for MockRightChildStats {
        fn build_snapshot(&self, _ordering: Ordering) -> StatSnapshot {
            StatSnapshot::Source(common_metrics::snapshot::SourceSnapshot {
                cpu_us: 0,
                rows_out: self.estimated_total,
                bytes_read: 0,
                estimated_total_rows: self.estimated_total,
            })
        }
        fn add_duration_us(&self, _: u64) {}
    }

    fn make_stats(right_child_estimated_total: u64) -> JoinStats {
        JoinStats::new(
            &Meter::test_scope("join_test"),
            &node_info_from_id(10),
            Arc::new(MockRightChildStats {
                estimated_total: right_child_estimated_total,
            }),
        )
    }

    #[test]
    fn join_progress_starts_at_zero() {
        let stats = make_stats(5000);
        let snap = stats.build_snapshot(Ordering::SeqCst);
        assert_eq!(snap.current_progress(), 0);
        assert_eq!(snap.total(), 5000);
        assert_eq!(snap.total_key(), "joined rows out");
    }

    #[test]
    fn join_tracks_build_and_probe_independently() {
        let stats = make_stats(10_000);

        // Build phase: insert 3000 rows into hash table
        stats.add_build_rows_inserted(3000);

        // Probe phase: 5000 rows probed, 4000 matched
        stats.add_probe_rows_in(5000);
        stats.add_probe_rows_out(4000);

        let snap = stats.build_snapshot(Ordering::SeqCst);
        // Progress bar shows probe output
        assert_eq!(snap.current_progress(), 4000);
        // Total comes from the right child's estimate
        assert_eq!(snap.total(), 10_000);
    }

    #[test]
    fn join_message_shows_build_rows() {
        let stats = make_stats(1000);
        stats.add_build_rows_inserted(500);
        let snap = stats.build_snapshot(Ordering::SeqCst);
        let msg = snap.to_message();
        assert!(
            msg.contains("build rows inserted"),
            "expected 'build rows inserted' in message, got: {msg}"
        );
    }

    #[test]
    fn join_probe_rows_accumulate_across_batches() {
        let stats = make_stats(10_000);

        stats.add_probe_rows_in(1000);
        stats.add_probe_rows_out(800);

        stats.add_probe_rows_in(2000);
        stats.add_probe_rows_out(1500);

        let snap = stats.build_snapshot(Ordering::SeqCst);
        assert_eq!(snap.current_progress(), 2300); // 800 + 1500
    }
}
