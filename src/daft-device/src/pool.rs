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

/// Rounds a requested length up to its size class (the next power of two, at
/// least [`MIN_POOL_BLOCK`]). Pathological lengths whose next power of two
/// would overflow are used as-is, bypassing rounding.
fn request_class(len: usize) -> usize {
    let len = len.max(MIN_POOL_BLOCK);
    len.checked_next_power_of_two().unwrap_or(len)
}

/// The size class a freed block is stored under: the largest power of two not
/// exceeding its actual length, so every block in class `c` has at least `c`
/// usable bytes.
fn storage_class(len: usize) -> usize {
    debug_assert!(len > 0);
    1usize << (usize::BITS - 1 - len.leading_zeros())
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
/// Requests are rounded up to power-of-two size classes (min 256 bytes) and
/// reused only within their exact class; freed blocks above the configured
/// capacity are released to the backing allocator immediately.
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

impl DeviceAllocator for PooledDeviceAllocator {
    fn device(&self) -> Device {
        self.inner.device()
    }

    fn allocate(&self, len: usize, stream: DeviceStream) -> DaftResult<DeviceAllocation> {
        let class = request_class(len);
        {
            let mut state = self.state.lock().expect("pool lock poisoned");
            if let Some(block) = state
                .free_lists
                .get_mut(&(stream, class))
                .and_then(Vec::pop)
            {
                debug_assert!(block.len() >= class && class >= len);
                state.stats.hits += 1;
                state.stats.pooled_bytes -= block.len();
                state.stats.pooled_blocks -= 1;
                return Ok(block);
            }
            state.stats.misses += 1;
        }
        self.inner.allocate(class, stream)
    }

    unsafe fn deallocate(&self, allocation: DeviceAllocation, stream: DeviceStream) {
        let class = storage_class(allocation.len());
        {
            let mut state = self.state.lock().expect("pool lock poisoned");
            if state.stats.pooled_bytes + allocation.len() <= self.max_pool_bytes {
                state.stats.pooled_bytes += allocation.len();
                state.stats.pooled_blocks += 1;
                state
                    .free_lists
                    .entry((stream, class))
                    .or_default()
                    .push(allocation);
                return;
            }
            state.stats.evictions += 1;
        }
        // SAFETY: the caller's guarantees are forwarded unchanged: `allocation`
        // originated from `self.inner.allocate` (via `Self::allocate`) and is
        // not used after this free.
        unsafe { self.inner.deallocate(allocation, stream) };
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
        dst: &mut [u8],
        stream: DeviceStream,
    ) -> DaftResult<()> {
        // SAFETY: forwarded contract; pooled allocations are backed by `inner`.
        unsafe { self.inner.copy_device_to_host(src, dst, stream) }
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
mod tests {
    use super::*;
    use crate::{DeviceBuffer, HostAllocator, testing::EmulatedDeviceAllocator};

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
        assert_eq!(request_class(1 << 20), 1 << 20);
        // Overflowing sizes bypass rounding (and will fail at the backing
        // allocator rather than panic here).
        assert_eq!(request_class(usize::MAX), usize::MAX);

        assert_eq!(storage_class(256), 256);
        assert_eq!(storage_class(300), 256);
        assert_eq!(storage_class(512), 512);
        assert_eq!(storage_class(usize::MAX), 1 << (usize::BITS - 1));
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

    #[test]
    fn debug_includes_device_and_stats() {
        let allocator: Arc<dyn DeviceAllocator> = Arc::new(HostAllocator::new());
        let pool = PooledDeviceAllocator::new(allocator, 1024);
        let debug = format!("{pool:?}");
        assert!(debug.contains("max_pool_bytes"), "unexpected: {debug}");
    }
}
