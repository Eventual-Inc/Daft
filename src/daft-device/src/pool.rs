use std::{
    collections::HashMap,
    fmt,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;

use crate::{Device, DeviceAllocation, DeviceAllocator, DeviceStream, SyncEvent};

/// Smallest size class: sub-256-byte requests are rounded up to one block.
const MIN_POOL_BLOCK: usize = 256;

/// Largest power-of-two size class. Rounding to the next power of two wastes
/// up to 2x, which is unacceptable for large column chunks, so requests above
/// this are instead rounded up to a [`LARGE_CLASS_GRANULE`] multiple (waste
/// bounded by one granule).
const MAX_POW2_CLASS: usize = 1 << 20;

/// Granularity of size classes above [`MAX_POW2_CLASS`].
const LARGE_CLASS_GRANULE: usize = 64 * 1024;

/// Rounds a requested length up to its size class: the next power of two (at
/// least [`MIN_POOL_BLOCK`]) up to [`MAX_POW2_CLASS`], and the next
/// [`LARGE_CLASS_GRANULE`] multiple above that. Pathological lengths whose
/// rounding would overflow are used as-is, bypassing rounding.
fn request_class(len: usize) -> usize {
    if len <= MAX_POW2_CLASS {
        // Cannot overflow: len <= MAX_POW2_CLASS, itself a power of two.
        len.max(MIN_POOL_BLOCK).next_power_of_two()
    } else {
        len.checked_add(LARGE_CLASS_GRANULE - 1)
            .map_or(len, |bumped| {
                bumped / LARGE_CLASS_GRANULE * LARGE_CLASS_GRANULE
            })
    }
}

/// The size class a freed block is stored under: its length rounded *down*
/// (largest power of two, or largest granule multiple, not exceeding it), so
/// every block stored in class `c` has at least `c` usable bytes and can serve
/// any request of class `c`.
fn storage_class(len: usize) -> usize {
    debug_assert!(len > 0);
    if len <= MAX_POW2_CLASS {
        1usize << (usize::BITS - 1 - len.leading_zeros())
    } else {
        (len / LARGE_CLASS_GRANULE * LARGE_CLASS_GRANULE).max(MAX_POW2_CLASS)
    }
}

/// Point-in-time counters for a [`PooledDeviceAllocator`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Allocations served from the pool without touching the backing allocator.
    pub hits: u64,
    /// Allocations forwarded to the backing allocator.
    pub misses: u64,
    /// Freed blocks released to the backing allocator because retaining them
    /// would exceed the pool's capacity.
    pub evictions: u64,
    /// Bytes currently retained on free lists.
    pub pooled_bytes: usize,
    /// Blocks currently retained on free lists.
    pub pooled_blocks: usize,
}

#[derive(Default)]
struct PoolState {
    /// Free blocks keyed by `(stream, size class)`.
    free_lists: HashMap<(DeviceStream, usize), Vec<DeviceAllocation>>,
    stats: PoolStats,
}

/// A size-classed, stream-ordered pooling wrapper over any [`DeviceAllocator`].
///
/// Device allocation is expensive (a CUDA `cudaMalloc` synchronizes the
/// device), so per-morsel buffer churn needs pooling to stay off the critical
/// path. This pool mirrors the reuse rule of stream-ordered device pools such
/// as `cudaMallocAsync`: a block freed on a stream is reused, without any
/// device synchronization, only by later allocations on that *same* stream.
/// Cross-stream reuse would require event synchronization and is not
/// attempted — blocks stay on their stream's free list until an allocation on
/// that stream claims them, capacity pressure evicts them, or [`Self::trim`]
/// releases them.
///
/// Requests are rounded up to size classes — powers of two (min 256 bytes) up
/// to 1 MiB, then 64 KiB granules so large chunks waste at most one granule —
/// and reused only within their exact class; freed blocks above the configured
/// capacity are released to the backing allocator immediately.
///
/// Wrap only backends that expose *raw* allocation (`cuMemAlloc`-style APIs
/// with no internal cache). A backend already implemented over a self-pooling
/// allocator such as `cudaMallocAsync`/`cudaFreeAsync` should be registered
/// bare: stacking two pools with independent capacities and eviction policies
/// doubles retained memory and makes both caches fight each other.
pub struct PooledDeviceAllocator {
    inner: Arc<dyn DeviceAllocator>,
    max_pool_bytes: usize,
    state: Mutex<PoolState>,
}

impl fmt::Debug for PooledDeviceAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledDeviceAllocator")
            .field("device", &self.inner.device())
            .field("max_pool_bytes", &self.max_pool_bytes)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl PooledDeviceAllocator {
    /// Wraps `inner`, retaining up to `max_pool_bytes` of freed memory for
    /// reuse.
    pub fn new(inner: Arc<dyn DeviceAllocator>, max_pool_bytes: usize) -> Self {
        Self {
            inner,
            max_pool_bytes,
            state: Mutex::new(PoolState::default()),
        }
    }

    /// The wrapped backing allocator.
    pub fn inner(&self) -> &Arc<dyn DeviceAllocator> {
        &self.inner
    }

    /// Current pool counters.
    pub fn stats(&self) -> PoolStats {
        self.state.lock().expect("pool lock poisoned").stats
    }

    /// Releases every retained block to the backing allocator.
    pub fn trim(&self) {
        let drained: Vec<((DeviceStream, usize), Vec<DeviceAllocation>)> = {
            let mut state = self.state.lock().expect("pool lock poisoned");
            state.stats.pooled_bytes = 0;
            state.stats.pooled_blocks = 0;
            state.free_lists.drain().collect()
        };
        for ((stream, _), blocks) in drained {
            for block in blocks {
                // SAFETY: retained blocks came from `self.inner.allocate` and
                // leave the pool here, never to be used again. They were freed
                // to the pool ordered on `stream`, preserving stream ordering.
                unsafe { self.inner.deallocate(block, stream) };
            }
        }
    }
}

impl Drop for PooledDeviceAllocator {
    fn drop(&mut self) {
        self.trim();
    }
}

// SAFETY: every handed-out allocation originates from `inner.allocate` (pooled
// blocks have `len` >= their class >= the request), and all copies and events
// are forwarded to `inner` unchanged.
unsafe impl DeviceAllocator for PooledDeviceAllocator {
    fn device(&self) -> Device {
        self.inner.device()
    }

    fn allocate(&self, len: usize, stream: DeviceStream) -> DaftResult<DeviceAllocation> {
        let class = request_class(len);
        {
            let mut state = self.state.lock().expect("pool lock poisoned");
            let key = (stream, class);
            if let Some(list) = state.free_lists.get_mut(&key) {
                let block = list.pop().expect("free lists are never left empty");
                // Drop drained entries so `free_lists` only ever holds
                // non-empty lists (the eviction path relies on this).
                if list.is_empty() {
                    state.free_lists.remove(&key);
                }
                debug_assert!(block.len() >= class && class >= len);
                state.stats.hits += 1;
                state.stats.pooled_bytes -= block.len();
                state.stats.pooled_blocks -= 1;
                return Ok(block);
            }
            state.stats.misses += 1;
        }
        match self.inner.allocate(class, stream) {
            Ok(allocation) => Ok(allocation),
            Err(err) => {
                // The failure may be memory pressure we caused ourselves: the
                // backing allocator cannot see blocks retained on our free
                // lists. Release everything and retry once — the same
                // release-and-retry discipline device pools such as
                // `cudaMallocAsync` apply internally — before surfacing OOM.
                if self.stats().pooled_blocks == 0 {
                    return Err(err);
                }
                self.trim();
                self.inner.allocate(class, stream)
            }
        }
    }

    unsafe fn deallocate(&self, allocation: DeviceAllocation, stream: DeviceStream) {
        let class = storage_class(allocation.len());
        // Blocks released to the backing allocator, with the stream each was
        // pooled under (frees stay ordered on their own stream). Deallocation
        // happens after the pool lock is dropped.
        let mut released: Vec<(DeviceStream, DeviceAllocation)> = Vec::new();
        {
            let mut state = self.state.lock().expect("pool lock poisoned");
            if allocation.len() <= self.max_pool_bytes {
                // Make room by evicting retained blocks (arbitrary order):
                // freshly freed memory is the most likely to be requested
                // again, and this keeps blocks pooled under idle streams from
                // pinning the capacity forever.
                while state.stats.pooled_bytes + allocation.len() > self.max_pool_bytes {
                    let key = state
                        .free_lists
                        .keys()
                        .next()
                        .copied()
                        .expect("pooled_bytes > 0 implies a non-empty free list");
                    let mut list = state
                        .free_lists
                        .remove(&key)
                        .expect("key was just observed");
                    let block = list.pop().expect("free lists are never left empty");
                    if !list.is_empty() {
                        state.free_lists.insert(key, list);
                    }
                    state.stats.pooled_bytes -= block.len();
                    state.stats.pooled_blocks -= 1;
                    state.stats.evictions += 1;
                    released.push((key.0, block));
                }
                state.stats.pooled_bytes += allocation.len();
                state.stats.pooled_blocks += 1;
                state
                    .free_lists
                    .entry((stream, class))
                    .or_default()
                    .push(allocation);
            } else {
                // Larger than the whole pool: release it directly.
                state.stats.evictions += 1;
                released.push((stream, allocation));
            }
        }
        for (stream, block) in released {
            // SAFETY: the caller's guarantees are forwarded unchanged: every
            // released block originated from `self.inner.allocate` (via
            // `Self::allocate`), left the free lists above, and is never used
            // again; each free is ordered on the stream the block was last
            // freed on.
            unsafe { self.inner.deallocate(block, stream) };
        }
    }

    unsafe fn copy_host_to_device(
        &self,
        src: &[u8],
        dst: NonNull<u8>,
        stream: DeviceStream,
    ) -> DaftResult<Arc<dyn SyncEvent>> {
        // SAFETY: forwarded contract; pooled allocations are backed by `inner`.
        unsafe { self.inner.copy_host_to_device(src, dst, stream) }
    }

    unsafe fn copy_device_to_host(
        &self,
        src: NonNull<u8>,
        dst: NonNull<u8>,
        len: usize,
        stream: DeviceStream,
    ) -> DaftResult<()> {
        // SAFETY: forwarded contract; pooled allocations are backed by `inner`.
        unsafe { self.inner.copy_device_to_host(src, dst, len, stream) }
    }

    unsafe fn copy_device_to_device(
        &self,
        src: NonNull<u8>,
        dst: NonNull<u8>,
        len: usize,
        stream: DeviceStream,
    ) -> DaftResult<Arc<dyn SyncEvent>> {
        // SAFETY: forwarded contract; pooled allocations are backed by `inner`.
        unsafe { self.inner.copy_device_to_device(src, dst, len, stream) }
    }
}

#[cfg(test)]
// Tests exercise the explicit exit path (`copy_to_host`) directly; engine
// code must use `materialize_to_host` instead (enforced via clippy.toml).
#[allow(clippy::disallowed_methods)]
mod tests {
    use common_error::DaftError;

    use super::*;
    use crate::{DeviceBuffer, HostAllocator, SyncEvent, testing::EmulatedDeviceAllocator};

    fn pooled(max_pool_bytes: usize) -> (Arc<EmulatedDeviceAllocator>, PooledDeviceAllocator) {
        let emulated = Arc::new(EmulatedDeviceAllocator::default());
        let pool = PooledDeviceAllocator::new(emulated.clone(), max_pool_bytes);
        (emulated, pool)
    }

    #[test]
    fn size_classes() {
        assert_eq!(request_class(0), MIN_POOL_BLOCK);
        assert_eq!(request_class(1), MIN_POOL_BLOCK);
        assert_eq!(request_class(256), 256);
        assert_eq!(request_class(257), 512);
        assert_eq!(request_class(MAX_POW2_CLASS), MAX_POW2_CLASS);
        // Above MAX_POW2_CLASS: granule rounding, not power-of-two doubling.
        assert_eq!(
            request_class(MAX_POW2_CLASS + 1),
            MAX_POW2_CLASS + LARGE_CLASS_GRANULE
        );
        let big = 1_100_000_000; // ~1.1 GB must not become 2 GB
        let class = request_class(big);
        assert!(class >= big && class < big + LARGE_CLASS_GRANULE);
        assert_eq!(class % LARGE_CLASS_GRANULE, 0);
        // Overflowing sizes bypass rounding (and will fail at the backing
        // allocator rather than panic here).
        assert_eq!(request_class(usize::MAX), usize::MAX);

        assert_eq!(storage_class(256), 256);
        assert_eq!(storage_class(300), 256);
        assert_eq!(storage_class(512), 512);
        assert_eq!(storage_class(MAX_POW2_CLASS + 5), MAX_POW2_CLASS);
        assert_eq!(storage_class(class), class);
        assert_eq!(
            storage_class(usize::MAX),
            usize::MAX / LARGE_CLASS_GRANULE * LARGE_CLASS_GRANULE
        );

        // Every storable block can serve requests of its storage class.
        for len in [1, 256, 300, 4097, MAX_POW2_CLASS + 1, big, class] {
            assert!(storage_class(request_class(len)) >= len.max(1));
            assert!(request_class(len) >= len);
        }
    }

    #[test]
    fn large_blocks_are_reused_within_their_granule_class() {
        let (_, pool) = pooled(1 << 30);
        let stream = DeviceStream::DEFAULT;
        let len = MAX_POW2_CLASS + 3;

        let a = pool.allocate(len, stream).unwrap();
        let ptr = a.ptr();
        assert_eq!(a.len(), MAX_POW2_CLASS + LARGE_CLASS_GRANULE);
        // SAFETY: `a` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(a, stream) };

        let b = pool
            .allocate(MAX_POW2_CLASS + LARGE_CLASS_GRANULE, stream)
            .unwrap();
        assert_eq!(b.ptr(), ptr);
        // SAFETY: `b` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(b, stream) };
        assert_eq!(pool.stats().hits, 1);
    }

    #[test]
    fn same_stream_blocks_are_reused() {
        let (_, pool) = pooled(1 << 20);
        let stream = DeviceStream::DEFAULT;

        let a = pool.allocate(300, stream).unwrap();
        let ptr = a.ptr();
        assert_eq!(a.len(), 512);
        // SAFETY: `a` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(a, stream) };
        assert_eq!(pool.stats().pooled_blocks, 1);
        assert_eq!(pool.stats().pooled_bytes, 512);

        // A request in the same class on the same stream reuses the block.
        let b = pool.allocate(400, stream).unwrap();
        assert_eq!(b.ptr(), ptr);
        // SAFETY: `b` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(b, stream) };

        let stats = pool.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.evictions, 0);
    }

    #[test]
    fn cross_stream_blocks_are_not_reused() {
        let (_, pool) = pooled(1 << 20);
        let stream_a = DeviceStream::from_raw(1);
        let stream_b = DeviceStream::from_raw(2);

        let a = pool.allocate(1024, stream_a).unwrap();
        let ptr = a.ptr();
        // SAFETY: `a` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(a, stream_a) };

        // Same class, different stream: must not reuse without synchronization.
        let b = pool.allocate(1024, stream_b).unwrap();
        assert_ne!(b.ptr(), ptr);
        // SAFETY: `b` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(b, stream_b) };

        let stats = pool.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 2);
        assert_eq!(stats.pooled_blocks, 2);
    }

    #[test]
    fn capacity_overflow_evicts_to_inner() {
        let (emulated, pool) = pooled(512);
        let stream = DeviceStream::DEFAULT;

        let a = pool.allocate(512, stream).unwrap();
        let b = pool.allocate(512, stream).unwrap();
        assert_eq!(emulated.host().live_allocations(), 2);

        // SAFETY: both blocks came from `pool.allocate` and are unused after.
        unsafe {
            pool.deallocate(a, stream); // retained: fills the pool exactly
            pool.deallocate(b, stream); // evicted: pool is full
        }
        let stats = pool.stats();
        assert_eq!(stats.pooled_bytes, 512);
        assert_eq!(stats.evictions, 1);
        assert_eq!(emulated.host().live_allocations(), 1);
    }

    #[test]
    fn frees_evict_blocks_pooled_under_idle_streams() {
        let (emulated, pool) = pooled(1024);
        let dead_stream = DeviceStream::from_raw(1);
        let live_stream = DeviceStream::from_raw(2);

        // Fill the pool from a stream that then never allocates again.
        let a = pool.allocate(1024, dead_stream).unwrap();
        // SAFETY: `a` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(a, dead_stream) };
        assert_eq!(pool.stats().pooled_bytes, 1024);

        // A free on another stream evicts the idle stream's block instead of
        // being rejected, so pooling keeps working.
        let b = pool.allocate(1024, live_stream).unwrap();
        let ptr = b.ptr();
        // SAFETY: `b` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(b, live_stream) };
        let stats = pool.stats();
        assert_eq!(stats.evictions, 1);
        assert_eq!(stats.pooled_bytes, 1024);
        assert_eq!(emulated.host().live_allocations(), 1);

        let c = pool.allocate(1024, live_stream).unwrap();
        assert_eq!(c.ptr(), ptr);
        assert_eq!(pool.stats().hits, 1);
        // SAFETY: `c` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(c, live_stream) };
    }

    #[test]
    fn oversized_frees_are_released_directly() {
        let (emulated, pool) = pooled(512);
        let stream = DeviceStream::DEFAULT;
        let block = pool.allocate(4096, stream).unwrap();
        // SAFETY: `block` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(block, stream) };
        let stats = pool.stats();
        assert_eq!(stats.evictions, 1);
        assert_eq!(stats.pooled_bytes, 0);
        assert_eq!(emulated.host().live_allocations(), 0);
    }

    #[test]
    fn trim_and_drop_release_everything() {
        let (emulated, pool) = pooled(1 << 20);
        let stream = DeviceStream::DEFAULT;
        for len in [100, 1000, 10000] {
            let block = pool.allocate(len, stream).unwrap();
            // SAFETY: `block` came from `pool.allocate` and is unused after.
            unsafe { pool.deallocate(block, stream) };
        }
        assert_eq!(pool.stats().pooled_blocks, 3);
        assert!(emulated.host().live_allocations() > 0);

        pool.trim();
        assert_eq!(pool.stats().pooled_blocks, 0);
        assert_eq!(pool.stats().pooled_bytes, 0);
        assert_eq!(emulated.host().live_allocations(), 0);

        // Drop also trims.
        let block = pool.allocate(64, stream).unwrap();
        // SAFETY: `block` came from `pool.allocate` and is unused after.
        unsafe { pool.deallocate(block, stream) };
        drop(pool);
        assert_eq!(emulated.host().live_allocations(), 0);
    }

    #[test]
    fn pooled_buffers_round_trip_data() {
        let (emulated, pool) = pooled(1 << 20);
        let pool: Arc<dyn DeviceAllocator> = Arc::new(pool);
        let data: Vec<u8> = (0..200).collect();
        let buffer = DeviceBuffer::from_host(pool.clone(), &data, DeviceStream::DEFAULT).unwrap();
        assert_eq!(buffer.len(), 200);
        assert_eq!(buffer.copy_to_host().unwrap(), data);
        assert_eq!(
            buffer.slice(100, 50).unwrap().copy_to_host().unwrap(),
            data[100..150]
        );
        drop(buffer);
        drop(pool);
        assert_eq!(emulated.host().live_allocations(), 0);
    }

    #[test]
    fn concurrent_allocate_free_is_leak_free() {
        let (emulated, pool) = pooled(1 << 16);
        let pool = Arc::new(pool);
        let handles: Vec<_> = (0..8)
            .map(|thread_id: u64| {
                let pool = pool.clone();
                std::thread::spawn(move || {
                    let stream = DeviceStream::from_raw(thread_id % 2);
                    for i in 0..100 {
                        let len = 64 + (i % 7) * 500;
                        let block = pool.allocate(len, stream).unwrap();
                        assert!(block.len() >= len);
                        // SAFETY: `block` came from `pool.allocate` and is
                        // unused after this free.
                        unsafe { pool.deallocate(block, stream) };
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        let stats = pool.stats();
        assert_eq!(stats.hits + stats.misses, 800);
        pool.trim();
        assert_eq!(emulated.host().live_allocations(), 0);
    }

    /// Fails allocations that would push the backing store past `budget`,
    /// emulating device OOM so the trim-and-retry path is testable.
    #[derive(Debug)]
    struct BudgetAllocator {
        inner: EmulatedDeviceAllocator,
        budget: usize,
    }

    // SAFETY: storage, copies, and events delegate to
    // `EmulatedDeviceAllocator`; the budget check only decides whether to
    // allocate at all.
    unsafe impl DeviceAllocator for BudgetAllocator {
        fn device(&self) -> Device {
            self.inner.device()
        }

        fn allocate(&self, len: usize, stream: DeviceStream) -> DaftResult<DeviceAllocation> {
            if self.inner.host().live_bytes() + len > self.budget {
                return Err(DaftError::InternalError(format!(
                    "emulated device OOM: {len} bytes over budget"
                )));
            }
            self.inner.allocate(len, stream)
        }

        unsafe fn deallocate(&self, allocation: DeviceAllocation, stream: DeviceStream) {
            // SAFETY: forwarded contract; storage came from `self.inner`.
            unsafe { self.inner.deallocate(allocation, stream) }
        }

        unsafe fn copy_host_to_device(
            &self,
            src: &[u8],
            dst: NonNull<u8>,
            stream: DeviceStream,
        ) -> DaftResult<Arc<dyn SyncEvent>> {
            // SAFETY: forwarded contract.
            unsafe { self.inner.copy_host_to_device(src, dst, stream) }
        }

        unsafe fn copy_device_to_host(
            &self,
            src: NonNull<u8>,
            dst: NonNull<u8>,
            len: usize,
            stream: DeviceStream,
        ) -> DaftResult<()> {
            // SAFETY: forwarded contract.
            unsafe { self.inner.copy_device_to_host(src, dst, len, stream) }
        }

        unsafe fn copy_device_to_device(
            &self,
            src: NonNull<u8>,
            dst: NonNull<u8>,
            len: usize,
            stream: DeviceStream,
        ) -> DaftResult<Arc<dyn SyncEvent>> {
            // SAFETY: forwarded contract.
            unsafe { self.inner.copy_device_to_device(src, dst, len, stream) }
        }
    }

    #[test]
    fn allocation_failure_trims_and_retries() {
        let inner = Arc::new(BudgetAllocator {
            inner: EmulatedDeviceAllocator::default(),
            budget: 1024,
        });
        let pool = PooledDeviceAllocator::new(inner, 1 << 20);
        let stream = DeviceStream::DEFAULT;

        let a = pool.allocate(512, stream).unwrap();
        // SAFETY: `a` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(a, stream) };
        assert_eq!(pool.stats().pooled_bytes, 512);

        // The backing allocator sees 512 retained + 1024 requested > budget
        // and reports OOM; the pool must release its free lists and retry
        // rather than surface the failure.
        let b = pool.allocate(1024, stream).unwrap();
        assert!(b.len() >= 1024);
        assert_eq!(pool.stats().pooled_bytes, 0);
        // SAFETY: `b` came from `pool.allocate` and is not used afterwards.
        unsafe { pool.deallocate(b, stream) };

        // A request over budget even after trimming still fails.
        assert!(pool.allocate(4096, stream).is_err());
        assert_eq!(pool.stats().pooled_bytes, 0);
    }

    #[test]
    fn debug_includes_device_and_stats() {
        let allocator: Arc<dyn DeviceAllocator> = Arc::new(HostAllocator::new());
        let pool = PooledDeviceAllocator::new(allocator, 1024);
        let debug = format!("{pool:?}");
        assert!(debug.contains("max_pool_bytes"), "unexpected: {debug}");
    }
}
