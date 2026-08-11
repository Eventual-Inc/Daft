use std::{
    fmt,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

use common_error::{DaftError, DaftResult};

use crate::{
    Device, DeviceAllocation, DeviceAllocator, DeviceStream, ReadySyncEvent, SyncEvent,
    allocator_for,
};

/// The refcounted storage shared by a [`DeviceBuffer`] and all its slices.
struct DeviceBufferInner {
    /// `Some` until dropped; `Option` so `Drop` can move it into `deallocate`.
    allocation: Option<DeviceAllocation>,
    allocator: Arc<dyn DeviceAllocator>,
    stream: DeviceStream,
    /// Event guarding the most recent producer of this buffer's contents.
    sync_event: Mutex<Arc<dyn SyncEvent>>,
}

impl fmt::Debug for DeviceBufferInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceBufferInner")
            .field("allocation", &self.allocation)
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl Drop for DeviceBufferInner {
    fn drop(&mut self) {
        if let Some(allocation) = self.allocation.take() {
            // SAFETY: `allocation` was produced by `self.allocator` at
            // construction and this is the last owner, so it is never used
            // again. Consumers are responsible for ordering their reads before
            // dropping their reference (see `DeviceBuffer::synchronize`).
            unsafe { self.allocator.deallocate(allocation, self.stream) };
        }
    }
}

/// A refcounted device-resident byte buffer: the physical storage behind a
/// device-backed Series chunk, carrying `{ptr, len, device, sync_event}` per
/// the Arrow C Device model.
///
/// Cloning and [slicing](Self::slice) are zero-copy: both share the underlying
/// allocation, which is freed (stream-ordered) when the last reference drops.
/// Consumers must observe the [sync event](Self::synchronize) before reading
/// the buffer's contents.
///
/// The base allocation is aligned to at least
/// [`crate::MIN_DEVICE_ALIGNMENT`]; a slice's alignment is the base alignment
/// offset by the slice's start, so layouts that need aligned rows must place
/// them at aligned offsets (the ragged tensor design aligns row starts to 16
/// bytes at construction time).
#[derive(Clone, Debug)]
pub struct DeviceBuffer {
    inner: Arc<DeviceBufferInner>,
    offset: usize,
    len: usize,
}

impl DeviceBuffer {
    /// Allocates an uninitialized buffer of `len` bytes on `allocator`'s
    /// device, ordered on `stream`.
    pub fn allocate(
        allocator: Arc<dyn DeviceAllocator>,
        len: usize,
        stream: DeviceStream,
    ) -> DaftResult<Self> {
        let allocation = allocator.allocate(len, stream)?;
        debug_assert!(allocation.len() >= len);
        Ok(Self {
            inner: Arc::new(DeviceBufferInner {
                allocation: Some(allocation),
                allocator,
                stream,
                sync_event: Mutex::new(Arc::new(ReadySyncEvent)),
            }),
            offset: 0,
            len,
        })
    }

    /// Allocates an uninitialized buffer of `len` bytes on `device`, using the
    /// process-wide [registered allocator](crate::register_allocator).
    pub fn allocate_on(device: Device, len: usize, stream: DeviceStream) -> DaftResult<Self> {
        Self::allocate(allocator_for(device)?, len, stream)
    }

    /// Copies `data` from host memory into a new buffer on `allocator`'s
    /// device. The copy is ordered on `stream` and guarded by the buffer's
    /// [sync event](Self::synchronize).
    pub fn from_host(
        allocator: Arc<dyn DeviceAllocator>,
        data: &[u8],
        stream: DeviceStream,
    ) -> DaftResult<Self> {
        let buffer = Self::allocate(allocator, data.len(), stream)?;
        if !data.is_empty() {
            // SAFETY: `data_ptr` points at the start of a live allocation of at
            // least `data.len()` bytes, freshly allocated above so nothing else
            // accesses it.
            let event = unsafe {
                buffer
                    .inner
                    .allocator
                    .copy_host_to_device(data, buffer.data_ptr(), stream)?
            };
            buffer.set_sync_event(event);
        }
        Ok(buffer)
    }

    /// Copies `data` from host memory into a new buffer on `device`, using the
    /// process-wide [registered allocator](crate::register_allocator).
    pub fn from_host_on(device: Device, data: &[u8], stream: DeviceStream) -> DaftResult<Self> {
        Self::from_host(allocator_for(device)?, data, stream)
    }

    /// The device this buffer resides on.
    pub fn device(&self) -> Device {
        self.allocation().device()
    }

    /// The stream this buffer's lifetime and producing work are ordered on.
    pub fn stream(&self) -> DeviceStream {
        self.inner.stream
    }

    /// Length of this buffer (view) in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether this buffer (view) is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Pointer to this buffer's first byte — a device address for non-CPU
    /// devices, which must not be dereferenced from host code unless
    /// [`Device::is_host_accessible`].
    pub fn as_ptr(&self) -> NonNull<u8> {
        self.data_ptr()
    }

    /// Number of `DeviceBuffer` handles (clones and slices) sharing the
    /// underlying allocation.
    pub fn reference_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// A zero-copy view of `length` bytes starting at `offset` within this
    /// view. The slice shares (and keeps alive) the underlying allocation.
    pub fn slice(&self, offset: usize, length: usize) -> DaftResult<Self> {
        let end = offset.checked_add(length);
        if end.is_none_or(|end| end > self.len) {
            return Err(DaftError::ValueError(format!(
                "device buffer slice [{offset}, {offset}+{length}) out of bounds for length {}",
                self.len
            )));
        }
        Ok(Self {
            inner: self.inner.clone(),
            offset: self.offset + offset,
            len: length,
        })
    }

    /// The event guarding the most recent producer of this buffer's contents.
    pub fn sync_event(&self) -> Arc<dyn SyncEvent> {
        self.inner
            .sync_event
            .lock()
            .expect("sync_event lock poisoned")
            .clone()
    }

    /// Replaces the buffer's sync event. Called by producers (H2D copies,
    /// kernel launches) after enqueueing work that writes the buffer.
    ///
    /// Note that the event is shared by all clones/slices of the buffer.
    pub fn set_sync_event(&self, event: Arc<dyn SyncEvent>) {
        *self
            .inner
            .sync_event
            .lock()
            .expect("sync_event lock poisoned") = event;
    }

    /// Blocks the calling host thread until the work producing this buffer's
    /// contents has completed.
    pub fn synchronize(&self) -> DaftResult<()> {
        self.sync_event().synchronize()
    }

    /// Copies this buffer's contents to host memory, synchronizing on the
    /// buffer's sync event first.
    ///
    /// This is the *explicit* exit path (behind `to_cupy()` / `__dlpack__` /
    /// user-requested host conversion). Implicit engine-driven pullbacks must
    /// go through [`crate::materialize_to_host`] instead so they are counted.
    pub fn copy_to_host(&self) -> DaftResult<Vec<u8>> {
        self.synchronize()?;
        let mut out = vec![0u8; self.len];
        if !out.is_empty() {
            // SAFETY: `data_ptr` points into the live allocation with `len`
            // bytes available (slice construction is bounds-checked), and all
            // producing work was synchronized above.
            unsafe {
                self.inner.allocator.copy_device_to_host(
                    self.data_ptr(),
                    &mut out,
                    self.inner.stream,
                )?;
            }
        }
        Ok(out)
    }

    fn allocation(&self) -> &DeviceAllocation {
        self.inner
            .allocation
            .as_ref()
            .expect("allocation is present until drop")
    }

    fn data_ptr(&self) -> NonNull<u8> {
        let base = self.allocation().ptr();
        // SAFETY: `offset <= allocation.len()` by construction, so the result
        // is within (or one past the end of) the same allocation.
        unsafe { base.add(self.offset) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HostAllocator, MIN_DEVICE_ALIGNMENT, testing::EmulatedDeviceAllocator};

    fn host_backed() -> (Arc<EmulatedDeviceAllocator>, Arc<dyn DeviceAllocator>) {
        let allocator = Arc::new(EmulatedDeviceAllocator::default());
        let as_dyn: Arc<dyn DeviceAllocator> = allocator.clone();
        (allocator, as_dyn)
    }

    #[test]
    fn from_host_round_trips() {
        let (emulated, allocator) = host_backed();
        let data: Vec<u8> = (0..100).collect();
        let buffer = DeviceBuffer::from_host(allocator, &data, DeviceStream::DEFAULT).unwrap();
        assert_eq!(buffer.len(), 100);
        assert!(!buffer.is_empty());
        assert!(!buffer.device().is_cpu());
        assert_eq!(buffer.copy_to_host().unwrap(), data);
        drop(buffer);
        assert_eq!(emulated.host().live_allocations(), 0);
    }

    #[test]
    fn empty_buffer_round_trips() {
        let (_, allocator) = host_backed();
        let buffer = DeviceBuffer::from_host(allocator, &[], DeviceStream::DEFAULT).unwrap();
        assert!(buffer.is_empty());
        assert_eq!(buffer.copy_to_host().unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn clones_and_slices_share_the_allocation() {
        let (emulated, allocator) = host_backed();
        let data: Vec<u8> = (0..64).collect();
        let buffer = DeviceBuffer::from_host(allocator, &data, DeviceStream::DEFAULT).unwrap();
        assert_eq!(buffer.reference_count(), 1);

        let clone = buffer.clone();
        let slice = buffer.slice(16, 32).unwrap();
        assert_eq!(buffer.reference_count(), 3);
        assert_eq!(slice.len(), 32);
        assert_eq!(slice.copy_to_host().unwrap(), data[16..48]);
        // Slicing a slice re-bases against the view, not the allocation.
        let nested = slice.slice(8, 8).unwrap();
        assert_eq!(nested.copy_to_host().unwrap(), data[24..32]);

        // The allocation survives until the last reference drops.
        drop(buffer);
        drop(clone);
        assert_eq!(emulated.host().live_allocations(), 1);
        drop(slice);
        assert_eq!(emulated.host().live_allocations(), 1);
        drop(nested);
        assert_eq!(emulated.host().live_allocations(), 0);
    }

    #[test]
    fn slice_bounds_are_checked() {
        let (_, allocator) = host_backed();
        let buffer = DeviceBuffer::from_host(allocator, &[0u8; 16], DeviceStream::DEFAULT).unwrap();
        assert!(buffer.slice(0, 16).is_ok());
        assert!(buffer.slice(16, 0).is_ok());
        assert!(buffer.slice(0, 17).is_err());
        assert!(buffer.slice(17, 0).is_err());
        assert!(buffer.slice(8, 9).is_err());
        assert!(buffer.slice(usize::MAX, 2).is_err());
    }

    #[test]
    fn base_pointer_is_aligned() {
        let allocator: Arc<dyn DeviceAllocator> = Arc::new(HostAllocator::new());
        let buffer = DeviceBuffer::allocate(allocator, 128, DeviceStream::DEFAULT).unwrap();
        assert_eq!(buffer.as_ptr().as_ptr() as usize % MIN_DEVICE_ALIGNMENT, 0);
        assert_eq!(buffer.device(), Device::CPU);
        assert_eq!(buffer.stream(), DeviceStream::DEFAULT);
    }

    #[test]
    fn sync_event_is_replaceable_and_shared() {
        let (_, allocator) = host_backed();
        let buffer = DeviceBuffer::from_host(allocator, &[1u8; 8], DeviceStream::DEFAULT).unwrap();
        let slice = buffer.slice(0, 4).unwrap();
        let event: Arc<dyn SyncEvent> = Arc::new(ReadySyncEvent);
        buffer.set_sync_event(event.clone());
        assert!(Arc::ptr_eq(&slice.sync_event(), &event));
        assert!(buffer.synchronize().is_ok());
    }
}
