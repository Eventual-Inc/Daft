from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import torch


def get_torch_device() -> torch.device:
    """Get the best available PyTorch device for computation."""
    import torch

    # 1. CUDA GPU (if available) - for NVIDIA GPUs with CUDA support
    if torch.cuda.is_available():
        return torch.device("cuda")

    # 2. MPS (Metal Performance Shaders) - for Apple Silicon Macs
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return torch.device("mps")

    # 3. CPU - as fallback when no GPU acceleration is available
    return torch.device("cpu")
