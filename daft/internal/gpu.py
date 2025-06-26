from __future__ import annotations

import warnings


def _raw_device_count_nvml() -> int:
    """Return number of devices as reported by NVML or zero if NVML discovery/initialization failed.

    Inspired by PyTorch: https://github.com/pytorch/pytorch/blob/88e54de21976aa504e797e47f06b480b9108ef5c/torch/cuda/__init__.py#L711
    """
    from ctypes import CDLL, byref, c_int

    try:
        nvml_h = CDLL("libnvidia-ml.so.1")
    except OSError:
        return 0
    rc = nvml_h.nvmlInit()
    if rc != 0:
        warnings.warn("Can't initialize NVML, assuming no CUDA devices.")
        return 0
    dev_count = c_int(0)
    rc = nvml_h.nvmlDeviceGetCount_v2(byref(dev_count))
    if rc != 0:
        warnings.warn("Can't get nvml device count, assuming no CUDA devices.")
        return 0
    del nvml_h
    return dev_count.value


def cuda_visible_devices() -> list[str]:
    """Get the list of CUDA devices visible to the current process."""
    import os

    visible_devices_envvar = os.getenv("CUDA_VISIBLE_DEVICES")

    if visible_devices_envvar is None:
        return [str(i) for i in range(_raw_device_count_nvml())]

    return [device.strip() for device in visible_devices_envvar.split(",") if device.strip()]
