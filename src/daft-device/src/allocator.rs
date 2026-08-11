use std::{
    collections::HashMap,
    fmt,
    ptr::NonNull,
    sync::{Arc, LazyLock, RwLock},
};

use common_error::{DaftError, DaftResult};

use crate::{Device, DeviceStream, HostAllocator, SyncEvent};

/// Minimum alignment guaranteed by every [`DeviceAllocator::allocate`].
///
/// The ragged device tensor design requires row start offsets aligned to 16
/// bytes; allocations must therefore be at least 16-byte aligned so that packed
/// values buffers can honor per-row alignment relative to their base pointer.
pub const MIN_DEVICE_ALIGNMENT: usize = 16;

/// A raw allocation produced by a [`DeviceAllocator`].
///
/// This is a plain owning handle: it does not free itself. Refcounting and
/// automatic release live in [`crate::DeviceBuffer`]; backends and pools move
/// `DeviceAllocation`s between [`DeviceAllocator::allocate`] and
/// [`DeviceAllocator::deallocate`].
#[derive(Debug)]
pub struct DeviceAllocation {
    ptr: NonNull<u8>,
    len: usize,
    device: Device,
}

// SAFETY: `DeviceAllocation` is an owning handle to raw device memory. The
// pointee is never dereferenced through this type; all access goes through
// `DeviceAllocator` copy methods whose callers uphold cross-thread
// synchronization via the stream/event contract.
unsafe impl Send for DeviceAllocation {}
// SAFETY: see the `Send` justification above; `&DeviceAllocation` exposes only
// the pointer value, length, and device, all of which are immutable.
unsafe impl Sync for DeviceAllocation {}

impl DeviceAllocation {
    /// Creates an allocation handle from its raw parts.
    ///
    /// `len` is the *actual* allocated byte length, which may exceed what was
    /// requested from [`DeviceAllocator::allocate`].
    pub fn new(ptr: NonNull<u8>, len: usize, device: Device) -> Self {
        Self { ptr, len, device }
    }

    /// Base pointer of the allocation (a device address for non-CPU devices).
    pub fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    /// Actual allocated byte length.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the allocation has zero length.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The device the memory resides on.
    pub fn device(&self) -> Device {
        self.device
    }
}

/// Stream-ordered allocation and copy interface for one device.
///
/// A backend (CUDA, ROCm, ...) implements this once; everything above it —
/// pooling ([`crate::PooledDeviceAllocator`]), refcounted buffers
/// ([`crate::DeviceBuffer`]), and governed pullback
/// ([`crate::materialize_to_host`]) — is backend-agnostic. [`HostAllocator`]
/// is the built-in CPU implementation.
///
/// All operations are stream-ordered (see [`DeviceStream`]); host backends may
/// implement them synchronously.
pub trait DeviceAllocator: Send + Sync + fmt::Debug {
    /// The device this allocator serves.
    fn device(&self) -> Device;

    /// Allocates at least `len` bytes, aligned to at least
    /// [`MIN_DEVICE_ALIGNMENT`], ordered on `stream`.
    ///
    /// The returned allocation's [`DeviceAllocation::len`] may exceed `len`.
    /// A zero-byte request returns a valid, unique allocation.
    fn allocate(&self, len: usize, stream: DeviceStream) -> DaftResult<DeviceAllocation>;

    /// Frees an allocation, ordered on `stream`.
    ///
    /// # Safety
    ///
    /// `allocation` must have been produced by [`DeviceAllocator::allocate`] on
    /// this same allocator instance and must not be used afterwards. All device
    /// work reading or writing the memory must be ordered before this free on
    /// `stream`.
    unsafe fn deallocate(&self, allocation: DeviceAllocation, stream: DeviceStream);

    /// Enqueues a copy of `src` from host memory to device memory at `dst`,
    /// ordered on `stream`. The returned event completes when the copy has
    /// finished; until then, `src` must remain valid and unmodified.
    ///
    /// # Safety
    ///
    /// `dst` must point into a live allocation from this allocator with at
    /// least `src.len()` bytes available, and no concurrent device work may
    /// access that region except as ordered on `stream`.
    unsafe fn copy_host_to_device(
        &self,
        src: &[u8],
        dst: NonNull<u8>,
        stream: DeviceStream,
    ) -> DaftResult<Arc<dyn SyncEvent>>;

    /// Copies `dst.len()` bytes from device memory at `src` into `dst`,
    /// completing synchronously: when this returns, `dst` holds the data.
    ///
    /// # Safety
    ///
    /// `src` must point into a live allocation from this allocator with at
    /// least `dst.len()` bytes available, and all device work writing that
    /// region must be ordered before this copy on `stream`.
    unsafe fn copy_device_to_host(
        &self,
        src: NonNull<u8>,
        dst: &mut [u8],
        stream: DeviceStream,
    ) -> DaftResult<()>;

    /// Enqueues a device-to-device copy of `len` bytes from `src` to `dst`,
    /// ordered on `stream`. This is the primitive behind on-device compaction
    /// gathers. The returned event completes when the copy has finished.
    ///
    /// # Safety
    ///
    /// `src` and `dst` must each point into live allocations from this
    /// allocator with at least `len` bytes available, the two regions must not
    /// overlap, and no concurrent device work may access them except as
    /// ordered on `stream`.
    unsafe fn copy_device_to_device(
        &self,
        src: NonNull<u8>,
        dst: NonNull<u8>,
        len: usize,
        stream: DeviceStream,
    ) -> DaftResult<Arc<dyn SyncEvent>>;
}

static ALLOCATOR_REGISTRY: LazyLock<RwLock<HashMap<Device, Arc<dyn DeviceAllocator>>>> =
    LazyLock::new(|| {
        let mut registry: HashMap<Device, Arc<dyn DeviceAllocator>> = HashMap::new();
        registry.insert(Device::CPU, Arc::new(HostAllocator::new()));
        RwLock::new(registry)
    });

/// Registers `allocator` as the process-wide allocator for its device,
/// replacing and returning any previous registration.
///
/// A [`HostAllocator`] is pre-registered for [`Device::CPU`]. Device backends
/// (e.g. a CUDA extension) call this at initialization for each device they
/// serve.
pub fn register_allocator(allocator: Arc<dyn DeviceAllocator>) -> Option<Arc<dyn DeviceAllocator>> {
    let device = allocator.device();
    ALLOCATOR_REGISTRY
        .write()
        .expect("allocator registry lock poisoned")
        .insert(device, allocator)
}

/// Looks up the process-wide allocator for `device`.
///
/// Fails if no backend has registered an allocator for the device.
pub fn allocator_for(device: Device) -> DaftResult<Arc<dyn DeviceAllocator>> {
    ALLOCATOR_REGISTRY
        .read()
        .expect("allocator registry lock poisoned")
        .get(&device)
        .cloned()
        .ok_or_else(|| {
            DaftError::ValueError(format!(
                "no device allocator registered for device '{device}'; \
                 a device backend must call daft_device::register_allocator at initialization"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DeviceType,
        testing::{EMULATED_DEVICE, EmulatedDeviceAllocator},
    };

    #[test]
    fn cpu_allocator_is_pre_registered() {
        let allocator = allocator_for(Device::CPU).unwrap();
        assert_eq!(allocator.device(), Device::CPU);
    }

    #[test]
    fn unregistered_device_fails_lookup() {
        let missing = Device::new(DeviceType::Hexagon, 42);
        let err = allocator_for(missing).unwrap_err().to_string();
        assert!(err.contains("hexagon:42"), "unexpected error: {err}");
    }

    #[test]
    fn register_and_lookup_custom_device() {
        let allocator: Arc<dyn DeviceAllocator> = Arc::new(EmulatedDeviceAllocator::default());
        register_allocator(allocator);
        let found = allocator_for(EMULATED_DEVICE).unwrap();
        assert_eq!(found.device(), EMULATED_DEVICE);
    }
}
