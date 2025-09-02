from __future__ import annotations

import torch


def get_device() -> torch.device:
    """Get the best available PyTorch device for computation.

    This function automatically selects the optimal device in order of preference:
    1. CUDA GPU (if available) - for NVIDIA GPUs with CUDA support
    2. MPS (Metal Performance Shaders) - for Apple Silicon Macs
    3. CPU - as fallback when no GPU acceleration is available
    """
    device = (
        torch.device("cuda")
        if torch.cuda.is_available()
        else torch.device("mps")
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available()
        else torch.device("cpu")
    )
    return device
