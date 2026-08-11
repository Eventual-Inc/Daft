use std::fmt;

use common_error::{DaftError, DaftResult};

/// Device type codes shared by DLPack and the Arrow C Device data interface.
///
/// DLPack's `DLDeviceType` and Arrow's `ArrowDeviceType` define identical
/// values, so a [`Device`] round-trips through either interchange format
/// unchanged.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeviceType {
    /// CPU (host) memory.
    Cpu = 1,
    /// CUDA GPU device memory.
    Cuda = 2,
    /// CUDA pinned host memory (`cudaMallocHost`).
    CudaHost = 3,
    /// OpenCL device memory.
    OpenCl = 4,
    /// Vulkan buffer memory.
    Vulkan = 7,
    /// Metal device memory.
    Metal = 8,
    /// Verilog simulator buffer.
    Vpi = 9,
    /// ROCm GPU device memory.
    Rocm = 10,
    /// ROCm pinned host memory.
    RocmHost = 11,
    /// Reserved extension device.
    Ext = 12,
    /// CUDA managed/unified memory (`cudaMallocManaged`).
    CudaManaged = 13,
    /// Unified shared memory on a oneAPI device.
    OneApi = 14,
    /// WebGPU device memory.
    WebGpu = 15,
    /// Qualcomm Hexagon DSP memory.
    Hexagon = 16,
}

impl DeviceType {
    /// Parses a raw DLPack / Arrow C Device type code.
    pub fn from_code(code: i32) -> DaftResult<Self> {
        match code {
            1 => Ok(Self::Cpu),
            2 => Ok(Self::Cuda),
            3 => Ok(Self::CudaHost),
            4 => Ok(Self::OpenCl),
            7 => Ok(Self::Vulkan),
            8 => Ok(Self::Metal),
            9 => Ok(Self::Vpi),
            10 => Ok(Self::Rocm),
            11 => Ok(Self::RocmHost),
            12 => Ok(Self::Ext),
            13 => Ok(Self::CudaManaged),
            14 => Ok(Self::OneApi),
            15 => Ok(Self::WebGpu),
            16 => Ok(Self::Hexagon),
            other => Err(DaftError::ValueError(format!(
                "unknown DLPack/Arrow device type code: {other}"
            ))),
        }
    }

    /// The raw DLPack / Arrow C Device type code.
    pub fn code(self) -> i32 {
        self as i32
    }

    /// Whether memory of this device type is directly addressable from host code
    /// (so reads do not require a device-to-host copy).
    pub fn is_host_accessible(self) -> bool {
        matches!(
            self,
            Self::Cpu | Self::CudaHost | Self::RocmHost | Self::CudaManaged
        )
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Cuda => "cuda",
            Self::CudaHost => "cuda_host",
            Self::OpenCl => "opencl",
            Self::Vulkan => "vulkan",
            Self::Metal => "metal",
            Self::Vpi => "vpi",
            Self::Rocm => "rocm",
            Self::RocmHost => "rocm_host",
            Self::Ext => "ext",
            Self::CudaManaged => "cuda_managed",
            Self::OneApi => "oneapi",
            Self::WebGpu => "webgpu",
            Self::Hexagon => "hexagon",
        }
    }
}

impl fmt::Display for DeviceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Identity of a physical memory space: a device type plus an ordinal, matching
/// the `(device_type, device_id)` pair of DLPack and the Arrow C Device model.
///
/// Fields are private so every construction path goes through [`Self::new`],
/// which canonicalizes the identity: there is only one host memory space, so a
/// [`DeviceType::Cpu`] ordinal is normalized to 0 (interchange data is allowed
/// to carry an arbitrary CPU ordinal) and `Device::new(DeviceType::Cpu, 1) ==
/// Device::CPU` holds for equality, hashing, and allocator registry lookups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Device {
    device_type: DeviceType,
    device_id: i32,
}

impl Device {
    /// Host (CPU) memory.
    pub const CPU: Self = Self::new(DeviceType::Cpu, 0);

    /// Creates a device from a type and ordinal (normalizing the meaningless
    /// CPU ordinal to 0).
    pub const fn new(device_type: DeviceType, device_id: i32) -> Self {
        let device_id = match device_type {
            DeviceType::Cpu => 0,
            _ => device_id,
        };
        Self {
            device_type,
            device_id,
        }
    }

    /// The CUDA device with the given ordinal.
    pub const fn cuda(device_id: i32) -> Self {
        Self::new(DeviceType::Cuda, device_id)
    }

    /// The kind of device.
    pub const fn device_type(&self) -> DeviceType {
        self.device_type
    }

    /// Ordinal of the device within its type (e.g. CUDA device index).
    pub const fn device_id(&self) -> i32 {
        self.device_id
    }

    /// Whether this is host (CPU) memory.
    pub fn is_cpu(&self) -> bool {
        self.device_type == DeviceType::Cpu
    }

    /// Whether this device's memory is directly addressable from host code.
    pub fn is_host_accessible(&self) -> bool {
        self.device_type.is_host_accessible()
    }
}

impl fmt::Display for Device {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_cpu() {
            f.write_str("cpu")
        } else {
            write!(f, "{}:{}", self.device_type, self.device_id)
        }
    }
}

/// Opaque handle to a device execution stream (e.g. a `cudaStream_t`).
///
/// Allocations, frees, and copies in this crate are *stream-ordered*: they take
/// effect in the order work is enqueued on their stream. Memory freed on a
/// stream may be reused without synchronization only by allocations ordered
/// later on that same stream; reuse across streams requires event
/// synchronization. Host backends ignore stream ordering (host operations are
/// synchronous) but preserve the handles they are given.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct DeviceStream(u64);

impl DeviceStream {
    /// The backend's default stream (raw handle 0).
    pub const DEFAULT: Self = Self(0);

    /// Wraps a raw backend stream handle.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw backend stream handle.
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for DeviceStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stream:{:#x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_type_codes_round_trip() {
        for code in 1..=16 {
            match DeviceType::from_code(code) {
                Ok(device_type) => assert_eq!(device_type.code(), code),
                // 5 and 6 are unassigned in both DLPack and Arrow C Device.
                Err(_) => assert!(code == 5 || code == 6),
            }
        }
        assert!(DeviceType::from_code(0).is_err());
        assert!(DeviceType::from_code(17).is_err());
        assert!(DeviceType::from_code(-1).is_err());
    }

    #[test]
    fn device_display() {
        assert_eq!(Device::CPU.to_string(), "cpu");
        assert_eq!(Device::cuda(3).to_string(), "cuda:3");
        assert_eq!(Device::new(DeviceType::Rocm, 1).to_string(), "rocm:1");
    }

    #[test]
    fn cpu_ordinal_is_normalized() {
        assert_eq!(Device::new(DeviceType::Cpu, 3), Device::CPU);
        assert_eq!(Device::new(DeviceType::Cpu, 3).device_id(), 0);
        // Non-CPU ordinals are meaningful and preserved (including pinned host
        // memory, whose ordinal names the owning device).
        assert_eq!(Device::cuda(3).device_id(), 3);
        assert_eq!(Device::new(DeviceType::CudaHost, 2).device_id(), 2);
    }

    #[test]
    fn host_accessibility() {
        assert!(Device::CPU.is_host_accessible());
        assert!(Device::new(DeviceType::CudaHost, 0).is_host_accessible());
        assert!(Device::new(DeviceType::CudaManaged, 0).is_host_accessible());
        assert!(!Device::cuda(0).is_host_accessible());
        assert!(Device::CPU.is_cpu());
        assert!(!Device::cuda(0).is_cpu());
    }

    #[test]
    fn stream_raw_round_trip() {
        assert_eq!(DeviceStream::DEFAULT.raw(), 0);
        let stream = DeviceStream::from_raw(0xdead);
        assert_eq!(stream.raw(), 0xdead);
        assert_eq!(stream.to_string(), "stream:0xdead");
    }
}
