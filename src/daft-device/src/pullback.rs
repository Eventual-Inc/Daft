use std::{
    fmt,
    sync::{
        LazyLock,
        atomic::{AtomicU64, Ordering},
    },
};

use common_error::DaftResult;

use crate::DeviceBuffer;

/// Why the engine implicitly materialized a device-resident buffer to host.
///
/// Per-op host round trips are the dominant cost of device execution (measured
/// at 200–700x the kernel cost for elementwise ops at scale), so every implicit
/// pullback carries a reason: the resulting counters are the empirical priority
/// list for adding device kernel arms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PullbackReason {
    /// An op had no kernel arm for the buffer's (dtype, residency) combination.
    MissingDeviceKernel,
    /// The buffer crossed a host-only exchange boundary (e.g. a distributed
    /// shuffle; device buffers are node-local).
    Exchange,
    /// The buffer was serialized (IPC, network, disk), which requires host
    /// bytes.
    Serialization,
    /// Device memory pressure forced a spill to host.
    Spill,
}

impl PullbackReason {
    /// All reasons, in counter order.
    pub const ALL: [Self; 4] = [
        Self::MissingDeviceKernel,
        Self::Exchange,
        Self::Serialization,
        Self::Spill,
    ];

    fn index(self) -> usize {
        match self {
            Self::MissingDeviceKernel => 0,
            Self::Exchange => 1,
            Self::Serialization => 2,
            Self::Spill => 3,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::MissingDeviceKernel => "missing_device_kernel",
            Self::Exchange => "exchange",
            Self::Serialization => "serialization",
            Self::Spill => "spill",
        }
    }
}

impl fmt::Display for PullbackReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Default)]
struct ReasonCounters {
    count: AtomicU64,
    bytes: AtomicU64,
}

/// Counters of implicit device-to-host pullbacks, per [`PullbackReason`].
///
/// A process-wide instance ([`pullback_stats`]) is fed by
/// [`materialize_to_host`]; explain/analyze surfaces read it via
/// [`Self::snapshot`].
#[derive(Debug, Default)]
pub struct PullbackStats {
    counters: [ReasonCounters; PullbackReason::ALL.len()],
}

impl PullbackStats {
    /// Creates a zeroed counter set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one pullback of `bytes` bytes attributed to `reason`.
    pub fn record(&self, reason: PullbackReason, bytes: u64) {
        let counters = &self.counters[reason.index()];
        counters.count.fetch_add(1, Ordering::Relaxed);
        counters.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// A point-in-time copy of all counters.
    pub fn snapshot(&self) -> PullbackSnapshot {
        PullbackSnapshot {
            entries: PullbackReason::ALL.map(|reason| {
                let counters = &self.counters[reason.index()];
                (
                    reason,
                    counters.count.load(Ordering::Relaxed),
                    counters.bytes.load(Ordering::Relaxed),
                )
            }),
        }
    }

    /// Zeroes all counters.
    pub fn reset(&self) {
        for counters in &self.counters {
            counters.count.store(0, Ordering::Relaxed);
            counters.bytes.store(0, Ordering::Relaxed);
        }
    }
}

/// A point-in-time copy of [`PullbackStats`] counters: `(reason, count,
/// bytes)` per reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PullbackSnapshot {
    entries: [(PullbackReason, u64, u64); PullbackReason::ALL.len()],
}

impl PullbackSnapshot {
    /// Per-reason entries as `(reason, count, bytes)`.
    pub fn entries(&self) -> &[(PullbackReason, u64, u64)] {
        &self.entries
    }

    /// Number of pullbacks recorded for `reason`.
    pub fn count(&self, reason: PullbackReason) -> u64 {
        self.entries[reason.index()].1
    }

    /// Bytes pulled back for `reason`.
    pub fn bytes(&self, reason: PullbackReason) -> u64 {
        self.entries[reason.index()].2
    }

    /// Total pullbacks across all reasons.
    pub fn total_count(&self) -> u64 {
        self.entries.iter().map(|(_, count, _)| count).sum()
    }

    /// Total bytes pulled back across all reasons.
    pub fn total_bytes(&self) -> u64 {
        self.entries.iter().map(|(_, _, bytes)| bytes).sum()
    }
}

static GLOBAL_PULLBACK_STATS: LazyLock<PullbackStats> = LazyLock::new(PullbackStats::new);

/// The process-wide pullback counters fed by [`materialize_to_host`].
pub fn pullback_stats() -> &'static PullbackStats {
    &GLOBAL_PULLBACK_STATS
}

/// The one governed implicit device-to-host materialization.
///
/// Every engine code path that needs host bytes from a possibly
/// device-resident buffer — an op without a device kernel arm, a host-only
/// exchange, serialization, a spill — must exit through this function rather
/// than [`DeviceBuffer::copy_to_host`], so that every implicit pullback is
/// counted in [`pullback_stats`] and can be surfaced in explain/analyze.
/// (Explicit user-requested exits like `to_cupy()` / `__dlpack__` use
/// [`DeviceBuffer::copy_to_host`] directly and are not pullbacks.)
///
/// Buffers in host-accessible memory (CPU, pinned host, managed) are copied
/// out without being counted: there is no discrete device-to-host round trip
/// to attribute (managed-memory page migration is the device backend's cost,
/// not a pullback).
pub fn materialize_to_host(buffer: &DeviceBuffer, reason: PullbackReason) -> DaftResult<Vec<u8>> {
    let data = buffer.copy_to_host()?;
    if !buffer.device().is_host_accessible() {
        pullback_stats().record(reason, data.len() as u64);
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{DeviceAllocator, DeviceStream, HostAllocator, testing::EmulatedDeviceAllocator};

    #[test]
    fn local_stats_record_snapshot_reset() {
        let stats = PullbackStats::new();
        assert_eq!(stats.snapshot().total_count(), 0);

        stats.record(PullbackReason::MissingDeviceKernel, 100);
        stats.record(PullbackReason::MissingDeviceKernel, 50);
        stats.record(PullbackReason::Exchange, 8);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.count(PullbackReason::MissingDeviceKernel), 2);
        assert_eq!(snapshot.bytes(PullbackReason::MissingDeviceKernel), 150);
        assert_eq!(snapshot.count(PullbackReason::Exchange), 1);
        assert_eq!(snapshot.bytes(PullbackReason::Exchange), 8);
        assert_eq!(snapshot.count(PullbackReason::Spill), 0);
        assert_eq!(snapshot.total_count(), 3);
        assert_eq!(snapshot.total_bytes(), 158);
        assert_eq!(snapshot.entries().len(), PullbackReason::ALL.len());

        stats.reset();
        assert_eq!(stats.snapshot().total_count(), 0);
        assert_eq!(stats.snapshot().total_bytes(), 0);
    }

    #[test]
    fn reason_display() {
        assert_eq!(
            PullbackReason::MissingDeviceKernel.to_string(),
            "missing_device_kernel"
        );
        assert_eq!(PullbackReason::Spill.to_string(), "spill");
    }

    /// The only test that touches the global counters (tests run in parallel;
    /// nothing else calls `materialize_to_host`).
    #[test]
    fn global_pullback_accounting() {
        let device_allocator: Arc<dyn DeviceAllocator> =
            Arc::new(EmulatedDeviceAllocator::default());
        let host_allocator: Arc<dyn DeviceAllocator> = Arc::new(HostAllocator::new());
        let data: Vec<u8> = (0..32).collect();

        let before = pullback_stats().snapshot();

        // A device-resident buffer is counted, with its byte size.
        let device_buffer =
            DeviceBuffer::from_host(device_allocator, &data, DeviceStream::DEFAULT).unwrap();
        let out = materialize_to_host(&device_buffer, PullbackReason::Spill).unwrap();
        assert_eq!(out, data);

        // A CPU-resident buffer materializes fine but is not a pullback.
        let host_buffer =
            DeviceBuffer::from_host(host_allocator, &data, DeviceStream::DEFAULT).unwrap();
        let out = materialize_to_host(&host_buffer, PullbackReason::Serialization).unwrap();
        assert_eq!(out, data);

        // An explicit exit is never counted.
        let _ = device_buffer.copy_to_host().unwrap();

        let after = pullback_stats().snapshot();
        assert_eq!(
            after.count(PullbackReason::Spill),
            before.count(PullbackReason::Spill) + 1
        );
        assert_eq!(
            after.bytes(PullbackReason::Spill),
            before.bytes(PullbackReason::Spill) + 32
        );
        assert_eq!(
            after.count(PullbackReason::Serialization),
            before.count(PullbackReason::Serialization)
        );
        assert_eq!(after.total_count(), before.total_count() + 1);
    }
}
