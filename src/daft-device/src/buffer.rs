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
    /// Events guarding asynchronous device *reads* of this buffer (see
    /// [`DeviceBuffer::guard_read`]); the final release waits on these too.
    read_guards: Mutex<Vec<Arc<dyn SyncEvent>>>,
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
            // The sync event transitively guards all outstanding work on this
            // buffer (see `DeviceBuffer::set_sync_event`); wait on it so the
            // free cannot race producers still writing on other streams (the
            // free itself is only ordered against `self.stream`). Blocking the
            // host here is the deliberately conservative v1 policy: it only
            // stalls when a producer is still in flight at release time, and
            // an async backend can avoid even that by deferring frees behind
            // recorded events once that machinery exists. Best-effort: a sync
            // failure must not panic in drop.
            let event = self
                .sync_event
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if event.synchronize().is_err() {
                // If we cannot prove pending producers finished, freeing the
                // memory risks a use-after-free on the device; leaking the
                // allocation is the safer failure mode (dropping the handle
                // does not free the memory).
                return;
            }
            // Same policy for registered async readers: an in-flight read we
            // cannot prove finished may still be walking this memory.
            let read_guards = std::mem::take(
                self.read_guards
                    .get_mut()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
            if read_guards.iter().any(|guard| guard.synchronize().is_err()) {
                return;
            }
            // SAFETY: `allocation` was produced by `self.allocator` at
            // construction and this is the last owner, so it is never used
            // again. All producing work and every registered async read were
            // synchronized above; host-synchronous reads (`copy_to_host`)
            // complete before returning, and asynchronous device readers are
            // required to register their completion events via
            // `DeviceBuffer::guard_read`.
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
                read_guards: Mutex::new(Vec::new()),
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
    /// device.
    ///
    /// The copy is ordered on `stream` and *completes before this returns*:
    /// this is a safe API borrowing `data`, and the borrow must not end while
    /// an asynchronous backend could still be reading it. Zero-copy /
    /// asynchronous ingestion (DLPack, Arrow C Device) adopts externally owned
    /// memory instead of copying from a borrow and is a later slice.
    pub fn from_host(
        allocator: Arc<dyn DeviceAllocator>,
        data: &[u8],
        stream: DeviceStream,
    ) -> DaftResult<Self> {
        let buffer = Self::allocate(allocator, data.len(), stream)?;
        if !data.is_empty() {
            // SAFETY: `data_ptr` points at the start of a live allocation of at
            // least `data.len()` bytes, freshly allocated above so nothing else
            // accesses it, and `data` remains borrowed until the copy completes
            // (synchronized below).
            let event = unsafe {
                buffer
                    .inner
                    .allocator
                    .copy_host_to_device(data, buffer.data_ptr(), stream)?
            };
            // Install the copy's event as the buffer's producer guard *before*
            // waiting on it: if the wait fails, the error propagates and the
            // buffer drops, and the drop path must see the failing event (and
            // leak) rather than the initial always-ready event (and free under
            // a possibly still-running copy).
            buffer.set_sync_event(event.clone());
            event.synchronize()?;
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
    /// # Contract
    ///
    /// The new event must *transitively guard all previously enqueued work* on
    /// this buffer: before enqueueing new work, the producer must order it
    /// after the current [`Self::sync_event`] (e.g. by making its stream wait
    /// on that event), so that waiting on the newest event alone is always
    /// sufficient. [`Self::synchronize`] and the buffer's final release both
    /// rely on this. With concurrent producers, reading the current event and
    /// then calling this races (both may chain off the same predecessor and
    /// one guard is lost) — use [`Self::update_sync_event`], which performs
    /// the read-chain-store atomically.
    ///
    /// The event is shared by all clones/slices of the buffer.
    pub fn set_sync_event(&self, event: Arc<dyn SyncEvent>) {
        *self
            .inner
            .sync_event
            .lock()
            .expect("sync_event lock poisoned") = event;
    }

    /// Atomically replaces the sync event with one derived from the current
    /// event.
    ///
    /// The internal lock is held across `chain`, so concurrent producers
    /// serialize: each sees the event installed by the previous producer and
    /// must return an event ordered after both it and the newly enqueued work
    /// (e.g. by making the producing stream wait on the current event before
    /// enqueueing). Keep `chain` short — every clone/slice of the buffer
    /// blocks on this lock while it runs — and do not call back into this
    /// buffer's sync-event methods from inside it: the lock is held, so
    /// [`Self::sync_event`], [`Self::set_sync_event`], [`Self::synchronize`],
    /// or a nested `update_sync_event` would deadlock.
    pub fn update_sync_event(&self, chain: impl FnOnce(Arc<dyn SyncEvent>) -> Arc<dyn SyncEvent>) {
        let mut guard = self
            .inner
            .sync_event
            .lock()
            .expect("sync_event lock poisoned");
        let current = guard.clone();
        *guard = chain(current);
    }

    /// Blocks the calling host thread until the work producing this buffer's
    /// contents has completed.
    pub fn synchronize(&self) -> DaftResult<()> {
        self.sync_event().synchronize()
    }

    /// Registers `event` as the guard for an asynchronous device *read* of
    /// this buffer, so the final release cannot free the memory while that
    /// read is still in flight.
    ///
    /// This is the stamp-it-every-time pattern for consumers that enqueue
    /// device work *reading* the buffer on a stream not ordered with the
    /// buffer's own stream (e.g. a device-to-device gather feeding another
    /// buffer): enqueue the read, then hand its completion event here.
    /// Writers use [`Self::update_sync_event`] instead. Read guards are
    /// consulted only at final release — never by [`Self::synchronize`] or
    /// [`Self::copy_to_host`] — so readers do not wait on other readers.
    ///
    /// Events already reporting [`SyncEvent::is_complete`] are pruned
    /// opportunistically, so long-lived buffers do not accumulate guards for
    /// finished reads.
    pub fn guard_read(&self, event: Arc<dyn SyncEvent>) {
        let mut guards = self
            .inner
            .read_guards
            .lock()
            .expect("read_guards lock poisoned");
        guards.retain(|guard| !guard.is_complete());
        if !event.is_complete() {
            guards.push(event);
        }
    }

    /// Number of read guards currently registered (pending async reads).
    pub fn read_guard_count(&self) -> usize {
        self.inner
            .read_guards
            .lock()
            .expect("read_guards lock poisoned")
            .len()
    }

    /// Copies this buffer's contents to host memory, synchronizing on the
    /// buffer's sync event first.
    ///
    /// This is the *explicit* exit path (behind `to_cupy()` / `__dlpack__` /
    /// user-requested host conversion), tracked in the explicit-exit counters
    /// of [`crate::pullback_stats`]. Implicit engine-driven pullbacks must go
    /// through [`crate::materialize_to_host`] instead so they are attributed
    /// a [`crate::PullbackReason`] — the workspace `clippy.toml`
    /// disallowed-methods rule enforces this, and legitimate explicit exit
    /// sites carry an `#[allow(clippy::disallowed_methods)]` with a
    /// justification.
    pub fn copy_to_host(&self) -> DaftResult<Vec<u8>> {
        let data = self.copy_to_host_uncounted()?;
        if !self.device().is_host_accessible() {
            crate::pullback::record_explicit_exit(data.len() as u64);
        }
        Ok(data)
    }

    /// [`Self::copy_to_host`] without exit accounting: the shared primitive
    /// behind the explicit exit path and [`crate::materialize_to_host`],
    /// which each record their own counter.
    pub(crate) fn copy_to_host_uncounted(&self) -> DaftResult<Vec<u8>> {
        self.synchronize()?;
        let mut out = Vec::with_capacity(self.len);
        if self.len > 0 {
            let dst = NonNull::new(out.as_mut_ptr())
                .expect("Vec with non-zero capacity has a non-null pointer");
            // SAFETY: `data_ptr` points into the live allocation with `len`
            // bytes available (slice construction is bounds-checked), all
            // producing work was synchronized above, and `dst` is `len` bytes
            // of exclusively owned spare capacity. The copy completes
            // synchronously and initializes all `len` bytes, so `set_len` is
            // sound.
            unsafe {
                self.inner.allocator.copy_device_to_host(
                    self.data_ptr(),
                    dst,
                    self.len,
                    self.inner.stream,
                )?;
                out.set_len(self.len);
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
// Tests exercise the explicit exit path (`copy_to_host`) directly; engine
// code must use `materialize_to_host` instead (enforced via clippy.toml).
#[allow(clippy::disallowed_methods)]
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

    #[derive(Debug)]
    struct FailingSyncEvent;

    impl SyncEvent for FailingSyncEvent {
        fn synchronize(&self) -> DaftResult<()> {
            Err(DaftError::InternalError("device lost".to_string()))
        }
    }

    #[test]
    fn failed_synchronization_leaks_instead_of_freeing() {
        let (emulated, allocator) = host_backed();
        let buffer = DeviceBuffer::from_host(allocator, &[1u8; 8], DeviceStream::DEFAULT).unwrap();
        buffer.set_sync_event(Arc::new(FailingSyncEvent));
        assert!(buffer.synchronize().is_err());
        drop(buffer);
        // Intentionally leaked: without proof the producer finished, freeing
        // risks a device use-after-free, so the allocation must stay live.
        assert_eq!(emulated.host().live_allocations(), 1);
    }

    #[test]
    fn update_sync_event_chains_off_the_current_event() {
        let (_, allocator) = host_backed();
        let buffer = DeviceBuffer::from_host(allocator, &[0u8; 4], DeviceStream::DEFAULT).unwrap();
        let first: Arc<dyn SyncEvent> = Arc::new(ReadySyncEvent);
        buffer.set_sync_event(first.clone());
        buffer.update_sync_event(|current| {
            assert!(Arc::ptr_eq(&current, &first));
            Arc::new(ReadySyncEvent)
        });
        assert!(!Arc::ptr_eq(&buffer.sync_event(), &first));
        assert!(buffer.synchronize().is_ok());
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

    /// Emulated allocator whose H2D copies return an event that can never
    /// prove completion — as if the device were lost mid-copy.
    #[derive(Debug, Default)]
    struct FailingCopyAllocator {
        inner: EmulatedDeviceAllocator,
    }

    // SAFETY: storage and copies delegate to `EmulatedDeviceAllocator`; only
    // the H2D completion event differs, and returning a never-provable event
    // is contract-legal (events may fail to synchronize).
    unsafe impl DeviceAllocator for FailingCopyAllocator {
        fn device(&self) -> Device {
            self.inner.device()
        }

        fn allocate(&self, len: usize, stream: DeviceStream) -> DaftResult<DeviceAllocation> {
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
            // SAFETY: forwarded contract; `dst` points into host-backed storage.
            unsafe { self.inner.copy_host_to_device(src, dst, stream)? };
            Ok(Arc::new(FailingSyncEvent))
        }

        unsafe fn copy_device_to_host(
            &self,
            src: NonNull<u8>,
            dst: NonNull<u8>,
            len: usize,
            stream: DeviceStream,
        ) -> DaftResult<()> {
            // SAFETY: forwarded contract; `src` points into host-backed storage.
            unsafe { self.inner.copy_device_to_host(src, dst, len, stream) }
        }

        unsafe fn copy_device_to_device(
            &self,
            src: NonNull<u8>,
            dst: NonNull<u8>,
            len: usize,
            stream: DeviceStream,
        ) -> DaftResult<Arc<dyn SyncEvent>> {
            // SAFETY: forwarded contract; both pointers reference host-backed
            // storage.
            unsafe { self.inner.copy_device_to_device(src, dst, len, stream) }
        }
    }

    #[test]
    fn from_host_failure_leaks_instead_of_freeing() {
        let allocator = Arc::new(FailingCopyAllocator::default());
        let as_dyn: Arc<dyn DeviceAllocator> = allocator.clone();
        let result = DeviceBuffer::from_host(as_dyn, &[1u8; 8], DeviceStream::DEFAULT);
        assert!(result.is_err());
        // The copy's own event is the buffer's producer guard by the time the
        // wait fails, so the drop path leaks rather than freeing memory a
        // possibly still-running copy is writing.
        assert_eq!(allocator.inner.host().live_allocations(), 1);
    }

    #[test]
    fn pending_read_guard_blocks_release() {
        let (emulated, allocator) = host_backed();
        let buffer = DeviceBuffer::from_host(allocator, &[1u8; 8], DeviceStream::DEFAULT).unwrap();
        buffer.guard_read(Arc::new(FailingSyncEvent));
        assert_eq!(buffer.read_guard_count(), 1);
        // Producers are fully synchronized; the unproven *read* alone must
        // block the free.
        assert!(buffer.synchronize().is_ok());
        drop(buffer);
        assert_eq!(emulated.host().live_allocations(), 1);
    }

    #[test]
    fn completed_read_guards_are_pruned_and_do_not_block_release() {
        let (emulated, allocator) = host_backed();
        let buffer = DeviceBuffer::from_host(allocator, &[1u8; 8], DeviceStream::DEFAULT).unwrap();
        buffer.guard_read(Arc::new(ReadySyncEvent));
        assert_eq!(buffer.read_guard_count(), 0);
        drop(buffer);
        assert_eq!(emulated.host().live_allocations(), 0);
    }
}
