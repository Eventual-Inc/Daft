use std::{
    alloc::{Layout, alloc, dealloc},
    ptr::{self, NonNull},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use common_error::{DaftError, DaftResult};

use crate::{
    Device, DeviceAllocation, DeviceAllocator, DeviceStream, MIN_DEVICE_ALIGNMENT, ReadySyncEvent,
    SyncEvent,
};

/// Alignment of host allocations: one cache line, comfortably above
/// [`MIN_DEVICE_ALIGNMENT`].
const HOST_ALIGNMENT: usize = 64;

const _: () = assert!(HOST_ALIGNMENT >= MIN_DEVICE_ALIGNMENT);

/// The built-in CPU implementation of [`DeviceAllocator`].
///
/// Serves three roles: the [`Device::CPU`] arm of the residency model, the
/// destination of spill-to-host and pullback copies, and the test backend that
/// lets every allocator/buffer/pool code path run without a GPU. Host memory
/// has no stream ordering, so all operations complete synchronously and stream
/// handles are ignored.
///
/// Tracks live allocation counts and bytes so leaks are observable (and
/// assertable in tests).
#[derive(Debug, Default)]
pub struct HostAllocator {
    live_allocations: AtomicUsize,
    live_bytes: AtomicUsize,
}

impl HostAllocator {
    /// Creates a host allocator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of allocations currently outstanding.
    pub fn live_allocations(&self) -> usize {
        self.live_allocations.load(Ordering::Acquire)
    }

    /// Bytes currently outstanding.
    pub fn live_bytes(&self) -> usize {
        self.live_bytes.load(Ordering::Acquire)
    }

    fn layout_for(len: usize) -> DaftResult<Layout> {
        Layout::from_size_align(len, HOST_ALIGNMENT).map_err(|e| {
            DaftError::ValueError(format!("invalid host allocation of {len} bytes: {e}"))
        })
    }
}

// SAFETY: allocations come from the global allocator with `HOST_ALIGNMENT` and
// exact lengths; copies are synchronous memcpys that fully initialize their
// destinations; the returned events are always-complete `ReadySyncEvent`s.
unsafe impl DeviceAllocator for HostAllocator {
    fn device(&self) -> Device {
        Device::CPU
    }

    fn allocate(&self, len: usize, _stream: DeviceStream) -> DaftResult<DeviceAllocation> {
        // A zero-size `alloc` is undefined behavior; allocate one byte so every
        // allocation is a real, unique, freeable block.
        let alloc_len = len.max(1);
        let layout = Self::layout_for(alloc_len)?;
        // SAFETY: `layout` has non-zero size.
        let raw = unsafe { alloc(layout) };
        let Some(ptr) = NonNull::new(raw) else {
            return Err(DaftError::InternalError(format!(
                "host allocation of {alloc_len} bytes failed"
            )));
        };
        self.live_allocations.fetch_add(1, Ordering::AcqRel);
        self.live_bytes.fetch_add(alloc_len, Ordering::AcqRel);
        Ok(DeviceAllocation::new(ptr, alloc_len, Device::CPU))
    }

    unsafe fn deallocate(&self, allocation: DeviceAllocation, _stream: DeviceStream) {
        let layout =
            Self::layout_for(allocation.len()).expect("live allocation always has a valid layout");
        // SAFETY: the caller guarantees `allocation` came from `Self::allocate`
        // on this allocator, so its pointer was returned by `alloc` with this
        // exact layout (`len` is preserved by `DeviceAllocation`).
        unsafe { dealloc(allocation.ptr().as_ptr(), layout) };
        self.live_allocations.fetch_sub(1, Ordering::AcqRel);
        self.live_bytes
            .fetch_sub(allocation.len(), Ordering::AcqRel);
    }

    unsafe fn copy_host_to_device(
        &self,
        src: &[u8],
        dst: NonNull<u8>,
        _stream: DeviceStream,
    ) -> DaftResult<Arc<dyn SyncEvent>> {
        // SAFETY: the caller guarantees `dst` has at least `src.len()` bytes
        // available in a live allocation, and `src` is a valid borrowed slice.
        // Allocations are distinct from borrowed slices, so no overlap.
        unsafe { ptr::copy_nonoverlapping(src.as_ptr(), dst.as_ptr(), src.len()) };
        Ok(Arc::new(ReadySyncEvent))
    }

    unsafe fn copy_device_to_host(
        &self,
        src: NonNull<u8>,
        dst: NonNull<u8>,
        len: usize,
        _stream: DeviceStream,
    ) -> DaftResult<()> {
        // SAFETY: the caller guarantees `src` has at least `len` bytes
        // available in a live allocation, and `dst` is valid for `len` bytes
        // of writes with no overlap.
        unsafe { ptr::copy_nonoverlapping(src.as_ptr(), dst.as_ptr(), len) };
        Ok(())
    }

    unsafe fn copy_device_to_device(
        &self,
        src: NonNull<u8>,
        dst: NonNull<u8>,
        len: usize,
        _stream: DeviceStream,
    ) -> DaftResult<Arc<dyn SyncEvent>> {
        // SAFETY: the caller guarantees both regions are live, `len` bytes
        // large, and non-overlapping.
        unsafe { ptr::copy_nonoverlapping(src.as_ptr(), dst.as_ptr(), len) };
        Ok(Arc::new(ReadySyncEvent))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_and_deallocate_tracks_live_stats() {
        let allocator = HostAllocator::new();
        let a = allocator.allocate(100, DeviceStream::DEFAULT).unwrap();
        let b = allocator.allocate(200, DeviceStream::DEFAULT).unwrap();
        assert_eq!(allocator.live_allocations(), 2);
        assert_eq!(allocator.live_bytes(), 300);
        assert_eq!(a.device(), Device::CPU);
        assert!(a.len() >= 100);

        // SAFETY: allocations came from this allocator and are unused after.
        unsafe {
            allocator.deallocate(a, DeviceStream::DEFAULT);
            allocator.deallocate(b, DeviceStream::DEFAULT);
        }
        assert_eq!(allocator.live_allocations(), 0);
        assert_eq!(allocator.live_bytes(), 0);
    }

    #[test]
    fn allocations_are_aligned() {
        let allocator = HostAllocator::new();
        for len in [0, 1, 15, 16, 17, 1024] {
            let allocation = allocator.allocate(len, DeviceStream::DEFAULT).unwrap();
            let addr = allocation.ptr().as_ptr() as usize;
            assert_eq!(addr % MIN_DEVICE_ALIGNMENT, 0);
            assert_eq!(addr % HOST_ALIGNMENT, 0);
            // SAFETY: allocation came from this allocator and is unused after.
            unsafe { allocator.deallocate(allocation, DeviceStream::DEFAULT) };
        }
    }

    #[test]
    fn zero_len_allocations_are_unique() {
        let allocator = HostAllocator::new();
        let a = allocator.allocate(0, DeviceStream::DEFAULT).unwrap();
        let b = allocator.allocate(0, DeviceStream::DEFAULT).unwrap();
        assert_ne!(a.ptr(), b.ptr());
        assert!(a.is_empty() || a.len() == 1);
        // SAFETY: allocations came from this allocator and are unused after.
        unsafe {
            allocator.deallocate(a, DeviceStream::DEFAULT);
            allocator.deallocate(b, DeviceStream::DEFAULT);
        }
        assert_eq!(allocator.live_bytes(), 0);
    }

    #[test]
    fn copies_round_trip() {
        let allocator = HostAllocator::new();
        let src_data: Vec<u8> = (0..=255).collect();
        let a = allocator.allocate(256, DeviceStream::DEFAULT).unwrap();
        let b = allocator.allocate(256, DeviceStream::DEFAULT).unwrap();

        // SAFETY: both allocations are live with 256 bytes; regions are
        // distinct blocks so they cannot overlap.
        unsafe {
            let event = allocator
                .copy_host_to_device(&src_data, a.ptr(), DeviceStream::DEFAULT)
                .unwrap();
            event.synchronize().unwrap();

            let event = allocator
                .copy_device_to_device(a.ptr(), b.ptr(), 256, DeviceStream::DEFAULT)
                .unwrap();
            event.synchronize().unwrap();

            let mut out = vec![0u8; 256];
            let dst = NonNull::new(out.as_mut_ptr()).unwrap();
            allocator
                .copy_device_to_host(b.ptr(), dst, 256, DeviceStream::DEFAULT)
                .unwrap();
            assert_eq!(out, src_data);

            allocator.deallocate(a, DeviceStream::DEFAULT);
            allocator.deallocate(b, DeviceStream::DEFAULT);
        }
    }
}
