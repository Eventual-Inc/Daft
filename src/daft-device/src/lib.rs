//! Physical device residency foundation for Daft columns.
//!
//! This crate is the first slice of the device-resident columns design
//! (<https://github.com/Eventual-Inc/Daft/issues/7374>): the allocator, buffer,
//! and refcounting layer that device-backed Series chunk storage builds on.
//!
//! Residency is *physical, not logical*: schemas and dtypes never change, and
//! planning never sees residency. What changes is where a buffer's bytes live,
//! which this crate models with:
//!
//! - [`Device`] / [`DeviceType`]: the device identity model shared by DLPack and
//!   the Arrow C Device data interface (identical type codes).
//! - [`DeviceAllocator`]: a stream-ordered allocation and copy interface that a
//!   backend (e.g. CUDA) implements once. [`HostAllocator`] is the built-in CPU
//!   backend, which doubles as the test backend so every code path here runs in
//!   environments without a GPU.
//! - [`PooledDeviceAllocator`]: a size-classed pooling wrapper providing the
//!   stream-ordered reuse semantics of device memory pools such as
//!   `cudaMallocAsync`.
//! - [`DeviceBuffer`]: a refcounted, zero-copy-sliceable buffer carrying
//!   `{ptr, len, device, sync_event}` per the Arrow C Device model.
//! - [`materialize_to_host`]: the *one governed* implicit device-to-host
//!   pullback. Every op that lacks a device kernel arm must exit through this
//!   function so pullbacks are counted ([`pullback_stats`]) and can be surfaced
//!   in explain/analyze, forming the empirical priority list for adding device
//!   kernel arms.
//!
//! Explicit user-requested exits (`to_cupy()` / `__dlpack__` style) use
//! [`DeviceBuffer::copy_to_host`] directly and are not counted as pullbacks.

mod allocator;
mod buffer;
mod device;
mod event;
mod host;
mod pool;
mod pullback;

pub use allocator::{
    DeviceAllocation, DeviceAllocator, MIN_DEVICE_ALIGNMENT, allocator_for, register_allocator,
};
pub use buffer::DeviceBuffer;
pub use device::{Device, DeviceStream, DeviceType};
pub use event::{ReadySyncEvent, SyncEvent};
pub use host::HostAllocator;
pub use pool::{PoolStats, PooledDeviceAllocator};
pub use pullback::{
    PullbackReason, PullbackSnapshot, PullbackStats, materialize_to_host, pullback_stats,
};

#[cfg(test)]
pub(crate) mod testing {
    use std::{ptr::NonNull, sync::Arc};

    use common_error::DaftResult;

    use crate::{
        Device, DeviceAllocation, DeviceAllocator, DeviceStream, DeviceType, HostAllocator,
        SyncEvent,
    };

    /// A fake non-CPU device backed by host memory, so device-residency code
    /// paths (H2D/D2H copies, pullback accounting) are exercised without a GPU.
    pub const EMULATED_DEVICE: Device = Device::new(DeviceType::Ext, 7);

    /// [`DeviceAllocator`] for [`EMULATED_DEVICE`], delegating storage to a
    /// [`HostAllocator`] while reporting a non-CPU device.
    #[derive(Debug, Default)]
    pub struct EmulatedDeviceAllocator {
        host: HostAllocator,
    }

    impl EmulatedDeviceAllocator {
        pub fn host(&self) -> &HostAllocator {
            &self.host
        }
    }

    impl DeviceAllocator for EmulatedDeviceAllocator {
        fn device(&self) -> Device {
            EMULATED_DEVICE
        }

        fn allocate(&self, len: usize, stream: DeviceStream) -> DaftResult<DeviceAllocation> {
            let allocation = self.host.allocate(len, stream)?;
            Ok(DeviceAllocation::new(
                allocation.ptr(),
                allocation.len(),
                EMULATED_DEVICE,
            ))
        }

        unsafe fn deallocate(&self, allocation: DeviceAllocation, stream: DeviceStream) {
            let host_allocation =
                DeviceAllocation::new(allocation.ptr(), allocation.len(), Device::CPU);
            // SAFETY: `allocation` was produced by `Self::allocate`, whose storage
            // came from `self.host` with the same pointer and length.
            unsafe { self.host.deallocate(host_allocation, stream) }
        }

        unsafe fn copy_host_to_device(
            &self,
            src: &[u8],
            dst: NonNull<u8>,
            stream: DeviceStream,
        ) -> DaftResult<Arc<dyn SyncEvent>> {
            // SAFETY: forwarded contract; `dst` points into host-backed storage.
            unsafe { self.host.copy_host_to_device(src, dst, stream) }
        }

        unsafe fn copy_device_to_host(
            &self,
            src: NonNull<u8>,
            dst: &mut [u8],
            stream: DeviceStream,
        ) -> DaftResult<()> {
            // SAFETY: forwarded contract; `src` points into host-backed storage.
            unsafe { self.host.copy_device_to_host(src, dst, stream) }
        }

        unsafe fn copy_device_to_device(
            &self,
            src: NonNull<u8>,
            dst: NonNull<u8>,
            len: usize,
            stream: DeviceStream,
        ) -> DaftResult<Arc<dyn SyncEvent>> {
            // SAFETY: forwarded contract; both pointers reference host-backed storage.
            unsafe { self.host.copy_device_to_device(src, dst, len, stream) }
        }
    }
}
