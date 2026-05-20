use std::sync::atomic::Ordering;

use common_metrics::{
    Counter, Meter, SHUFFLE_BYTES_IN_KEY, SHUFFLE_BYTES_WRITTEN_KEY, SHUFFLE_FINALIZE_DURATION_KEY,
    SHUFFLE_PARTITIONS_WRITTEN_KEY, SHUFFLE_ROWS_IN_KEY, SHUFFLE_ROWS_WRITTEN_KEY, StatSnapshot,
    ops::NodeInfo, snapshot::ShuffleWriteSnapshot,
};
use opentelemetry::KeyValue;

use super::RuntimeStats;

pub struct ShuffleWriteRuntimeStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_written: Counter,
    bytes_in: Counter,
    bytes_written: Counter,
    partitions_written: Counter,
    finalize_duration_us: Counter,
    num_tasks: Counter,
    node_kv: Vec<KeyValue>,
}

impl RuntimeStats for ShuffleWriteRuntimeStats {
    fn new(meter: &Meter, node_info: &NodeInfo) -> Self {
        // duration_us/num_tasks reuse generic per-node metric names shared by all
        // operators; shuffle-specific counters use the shuffle.* prefix.
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.u64_counter(SHUFFLE_ROWS_IN_KEY),
            rows_written: meter.u64_counter(SHUFFLE_ROWS_WRITTEN_KEY),
            bytes_in: meter.u64_counter(SHUFFLE_BYTES_IN_KEY),
            bytes_written: meter.u64_counter(SHUFFLE_BYTES_WRITTEN_KEY),
            partitions_written: meter.u64_counter(SHUFFLE_PARTITIONS_WRITTEN_KEY),
            finalize_duration_us: meter.u64_counter(SHUFFLE_FINALIZE_DURATION_KEY),
            num_tasks: meter.num_tasks_metric(),
            node_kv: node_info.to_key_values(),
        }
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        StatSnapshot::ShuffleWrite(ShuffleWriteSnapshot {
            cpu_us: self.duration_us.load(ordering),
            rows_in: self.rows_in.load(ordering),
            rows_written: self.rows_written.load(ordering),
            bytes_in: self.bytes_in.load(ordering),
            bytes_written: self.bytes_written.load(ordering),
            partitions_written: self.partitions_written.load(ordering),
            finalize_duration_us: self.finalize_duration_us.load(ordering),
            num_tasks: self.num_tasks.load(ordering),
        })
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    // add_rows_out / add_bytes_out are required by the trait but represent the
    // generic "node produced output" concept. Shuffle writes track output via
    // add_rows_written / add_bytes_written, which we override below. The framework
    // drives all shuffle accounting through the *_written methods directly, so
    // these stay as no-ops — if some future non-BlockingSink path ever calls
    // add_rows_out on a ShuffleWriteRuntimeStats, silently dropping the value is
    // preferable to mis-attributing it against the typed rows_written counter.
    fn add_rows_out(&self, _rows: u64) {}

    fn add_duration_us(&self, duration_us: u64) {
        self.duration_us.add(duration_us, self.node_kv.as_slice());
    }

    fn add_bytes_in(&self, bytes: u64) {
        self.bytes_in.add(bytes, self.node_kv.as_slice());
    }

    fn add_bytes_out(&self, _bytes: u64) {}

    fn increment_num_tasks(&self) {
        self.num_tasks.add(1, self.node_kv.as_slice());
    }

    fn add_rows_written(&self, rows: u64) {
        self.rows_written.add(rows, self.node_kv.as_slice());
    }

    fn add_bytes_written(&self, bytes: u64) {
        self.bytes_written.add(bytes, self.node_kv.as_slice());
    }

    fn add_partitions_written(&self, partitions: u64) {
        self.partitions_written
            .add(partitions, self.node_kv.as_slice());
    }

    fn add_finalize_duration_us(&self, duration_us: u64) {
        self.finalize_duration_us
            .add(duration_us, self.node_kv.as_slice());
    }
}

#[cfg(test)]
mod tests {
    use common_metrics::{Meter, StatSnapshot, ops::NodeInfo};

    use super::*;

    #[test]
    fn shuffle_write_runtime_stats_round_trip() {
        let stats = ShuffleWriteRuntimeStats::new(
            &Meter::test_scope("shuffle_write_round_trip"),
            &NodeInfo::default(),
        );
        stats.add_rows_in(120);
        stats.add_bytes_in(2048);
        stats.add_rows_written(100);
        stats.add_bytes_written(1024);
        stats.add_partitions_written(4);
        stats.add_finalize_duration_us(750);
        stats.add_duration_us(900);
        stats.increment_num_tasks();
        stats.increment_num_tasks();

        let StatSnapshot::ShuffleWrite(snap) = stats.snapshot() else {
            panic!("expected ShuffleWrite snapshot");
        };
        assert_eq!(snap.rows_in, 120);
        assert_eq!(snap.bytes_in, 2048);
        assert_eq!(snap.rows_written, 100);
        assert_eq!(snap.bytes_written, 1024);
        assert_eq!(snap.partitions_written, 4);
        assert_eq!(snap.finalize_duration_us, 750);
        assert_eq!(snap.cpu_us, 900);
        assert_eq!(snap.num_tasks, 2);
    }

    /// Pins the no-op contract documented at the top of the `RuntimeStats` impl:
    /// `add_rows_out` / `add_bytes_out` must not increment `rows_written` /
    /// `bytes_written` via the trait's delegating defaults. A future "helpful"
    /// change that routes generic outputs into the typed counters would silently
    /// double-count shuffle writes and this test would catch it.
    #[test]
    fn shuffle_write_add_rows_out_is_noop() {
        let stats = ShuffleWriteRuntimeStats::new(
            &Meter::test_scope("shuffle_write_noop"),
            &NodeInfo::default(),
        );
        stats.add_rows_written(100);
        stats.add_bytes_written(1024);
        stats.add_rows_out(999);
        stats.add_bytes_out(9999);

        let StatSnapshot::ShuffleWrite(snap) = stats.snapshot() else {
            panic!("expected ShuffleWrite snapshot");
        };
        assert_eq!(snap.rows_written, 100);
        assert_eq!(snap.bytes_written, 1024);
    }
}
