from __future__ import annotations


def _raw_device_count_nvml() -> int:
    """
    Return number of devices as reported by NVML or zero if NVML discovery/initialization failed.

    Inspired by PyTorch: https://github.com/pytorch/pytorch/blob/88e54de21976aa504e797e47f06b480b9108ef5c/torch/cuda/__init__.py#L711
    """
    from ctypes import CDLL, byref, c_int

    nvml_h = CDLL("libnvidia-ml.so.1")
    rc = nvml_h.nvmlInit()
    if rc != 0:
        return 0
    dev_count = c_int(0)
    rc = nvml_h.nvmlDeviceGetCount_v2(byref(dev_count))
    if rc != 0:
        return 0
    del nvml_h
    return dev_count.value


def _parse_visible_devices() -> list[str] | None:
    """Parse CUDA_VISIBLE_DEVICES environment variable. Returns None if not set."""
    import os

    var = os.getenv("CUDA_VISIBLE_DEVICES")
    if var is None:
        return None
    else:
        return [device.strip() for device in var.split(",") if device.strip()]


def cuda_visible_devices() -> list[str]:
    """Get the list of CUDA devices visible to the current process."""
    visible_devices = _parse_visible_devices()
    if visible_devices is not None:
        return visible_devices
    else:
        return [str(i) for i in range(_raw_device_count_nvml())]
