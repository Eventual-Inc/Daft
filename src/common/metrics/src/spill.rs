use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use opentelemetry::KeyValue;

use crate::{
    Counter, Meter, SPILL_BYTES_READ_STAT_KEY, SPILL_BYTES_WRITTEN_STAT_KEY,
    SPILL_FILE_COUNT_STAT_KEY,
    ops::NodeInfo,
    snapshot::{SpillSnapshot, SpillSource},
};

/// Records per-operator spill I/O. Shared (via `Arc`) between the component
/// that owns the runtime stats and the I/O layer doing the actual disk work.
///
/// A no-op reporter is returned by `noop()` for callers that don't participate
/// in spill accounting (e.g. test harnesses, operators that never spill). All
/// record calls on a no-op reporter are branch-free on the hot path — they
/// check the `Option` inner.
#[derive(Clone)]
pub struct SpillReporter {
    inner: Option<Arc<SpillReporterInner>>,
}

impl std::fmt::Debug for SpillReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillReporter")
            .field("attached", &self.inner.is_some())
            .finish()
    }
}

struct SpillReporterInner {
    source: SpillSource,
    bytes_written: Counter,
    bytes_read: Counter,
    file_count: Counter,
    /// Plain atomic because `files_resident` can decrement (spill files are
    /// read back and deleted). TODO: expose as an OTel gauge via an
    /// observable callback so exporters (Prometheus, OTLP) can see residency
    /// — right now it only surfaces in snapshot payloads.
    files_resident: AtomicU64,
    node_kv: Vec<KeyValue>,
}

impl SpillReporter {
    pub fn new(meter: &Meter, node_info: &NodeInfo, source: SpillSource) -> Self {
        let inner = SpillReporterInner {
            source,
            bytes_written: meter.u64_counter(SPILL_BYTES_WRITTEN_STAT_KEY),
            bytes_read: meter.u64_counter(SPILL_BYTES_READ_STAT_KEY),
            file_count: meter.u64_counter(SPILL_FILE_COUNT_STAT_KEY),
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

    pub fn record_file_created(&self) {
        if let Some(inner) = &self.inner {
            inner.file_count.add(1, inner.node_kv.as_slice());
            inner.files_resident.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Decrement the resident-files gauge when a spill file is read back or
    /// deleted. Callers that leak files to end-of-query can skip this;
    /// `files_resident` will equal `file_count` in that case.
    pub fn record_file_removed(&self) {
        if let Some(inner) = &self.inner {
            let _ = inner
                .files_resident
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                });
        }
    }

    /// Produce a snapshot of the recorded metrics. Returns `None` on the
    /// no-op reporter and when no spill activity has been recorded (so
    /// snapshots that never spilled don't carry a zero-valued field).
    pub fn snapshot(&self, ordering: Ordering) -> Option<SpillSnapshot> {
        let inner = self.inner.as_ref()?;
        let bytes_written = inner.bytes_written.load(ordering);
        let bytes_read = inner.bytes_read.load(ordering);
        let file_count = inner.file_count.load(ordering);
        let files_resident = inner.files_resident.load(ordering);

        if bytes_written == 0 && bytes_read == 0 && file_count == 0 && files_resident == 0 {
            return None;
        }

        Some(SpillSnapshot {
            source: inner.source,
            bytes_written,
            bytes_read,
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
