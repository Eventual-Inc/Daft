use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use opentelemetry::KeyValue;

use crate::{
    Counter, Meter,
    meters::{
        SPILL_BYTES_READ_KEY, SPILL_BYTES_WRITTEN_KEY, SPILL_FILE_COUNT_KEY,
        SPILL_READ_DURATION_NS_KEY, SPILL_WRITE_DURATION_NS_KEY,
    },
    ops::NodeInfo,
    snapshot::{SpillMetrics, SpillSource},
};

/// Records per-operator spill I/O. Shared (via `Arc`) between the sink that
/// owns the runtime stats and the I/O layer doing the actual disk work.
///
/// A no-op reporter is returned by `noop()` for callers that don't participate
/// in spill accounting (e.g. test harnesses). No-op reporter calls are branch-free
/// on the hot path since they check the `Option` inner.
#[derive(Clone)]
pub struct SpillReporter {
    inner: Option<Arc<SpillReporterInner>>,
}

struct SpillReporterInner {
    source: SpillSource,
    bytes_written: Counter,
    bytes_read: Counter,
    write_duration_ns: Counter,
    read_duration_ns: Counter,
    file_count: Counter,
    // Plain atomic because files_resident can go down; we only care about
    // current value, not cumulative count. OTel instrumentation would need
    // a gauge callback rather than a counter.
    files_resident: AtomicU64,
    node_kv: Vec<KeyValue>,
}

impl SpillReporter {
    pub fn new(meter: &Meter, node_info: &NodeInfo, source: SpillSource) -> Self {
        let inner = SpillReporterInner {
            source,
            bytes_written: meter.u64_counter(SPILL_BYTES_WRITTEN_KEY),
            bytes_read: meter.u64_counter(SPILL_BYTES_READ_KEY),
            write_duration_ns: meter.u64_counter(SPILL_WRITE_DURATION_NS_KEY),
            read_duration_ns: meter.u64_counter(SPILL_READ_DURATION_NS_KEY),
            file_count: meter.u64_counter(SPILL_FILE_COUNT_KEY),
            files_resident: AtomicU64::new(0),
            node_kv: node_info.to_key_values(),
        };
        Self {
            inner: Some(Arc::new(inner)),
        }
    }

    pub fn noop() -> Self {
        Self { inner: None }
    }

    pub fn record_bytes_written(&self, bytes: u64) {
        if let Some(inner) = &self.inner {
            inner.bytes_written.add(bytes, inner.node_kv.as_slice());
        }
    }

    pub fn record_bytes_read(&self, bytes: u64) {
        if let Some(inner) = &self.inner {
            inner.bytes_read.add(bytes, inner.node_kv.as_slice());
        }
    }

    pub fn record_write_duration_ns(&self, ns: u64) {
        if let Some(inner) = &self.inner {
            inner.write_duration_ns.add(ns, inner.node_kv.as_slice());
        }
    }

    pub fn record_read_duration_ns(&self, ns: u64) {
        if let Some(inner) = &self.inner {
            inner.read_duration_ns.add(ns, inner.node_kv.as_slice());
        }
    }

    pub fn record_file_created(&self) {
        if let Some(inner) = &self.inner {
            inner.file_count.add(1, inner.node_kv.as_slice());
            inner.files_resident.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Decrement the resident-files gauge when a spill file is read back /
    /// deleted. Callers that never clean up (leak files to end-of-query) can
    /// skip this; `files_resident` will equal `file_count` in that case.
    pub fn record_file_removed(&self) {
        if let Some(inner) = &self.inner {
            inner.files_resident.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Produce a snapshot of the recorded metrics. Returns `None` for the no-op
    /// reporter and when no spill activity has been recorded.
    pub fn snapshot(&self, ordering: Ordering) -> Option<SpillMetrics> {
        let inner = self.inner.as_ref()?;
        let bytes_written = inner.bytes_written.load(ordering);
        let bytes_read = inner.bytes_read.load(ordering);
        let write_duration_ns = inner.write_duration_ns.load(ordering);
        let read_duration_ns = inner.read_duration_ns.load(ordering);
        let file_count = inner.file_count.load(ordering);
        let files_resident = inner.files_resident.load(ordering);

        if bytes_written == 0
            && bytes_read == 0
            && write_duration_ns == 0
            && read_duration_ns == 0
            && file_count == 0
            && files_resident == 0
        {
            return None;
        }

        Some(SpillMetrics {
            source: inner.source,
            bytes_written,
            bytes_read,
            write_duration_ns,
            read_duration_ns,
            file_count,
            files_resident,
        })
    }
}

impl Default for SpillReporter {
    fn default() -> Self {
        Self::noop()
    }
}
