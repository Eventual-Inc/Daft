from __future__ import annotations

from typing import TYPE_CHECKING

from daft.ai.typing import UDFOptions
from daft.daft import get_or_infer_runner_type

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


def get_gpu_udf_options() -> UDFOptions:
    """Get UDF options for GPU-based providers."""
    runner = get_or_infer_runner_type()

    # If native runner, use the number of GPUs visible to the current process
    if runner == "native":
        from daft.internal.gpu import cuda_visible_devices

        num_gpus = len(cuda_visible_devices())
    # If ray runner, use the number of GPUs currently on the cluster
    elif runner == "ray":
        import ray

        num_gpus = 0
        for node in ray.nodes():
            if "Resources" in node:
                if "GPU" in node["Resources"] and node["Resources"]["GPU"] > 0:
                    num_gpus += int(node["Resources"]["GPU"])
    else:
        raise ValueError(f"Invalid runner type: {runner}, expected 'native' or 'ray'")

    # If there are GPUs, set concurrency to the number of GPUs and num_gpus to 1
    if num_gpus > 0:
        return UDFOptions(concurrency=num_gpus, num_gpus=1)
    # If there are no GPUs, set concurrency to None and num_gpus to None, this will
    # make the UDF run on CPU threads
    else:
        return UDFOptions(concurrency=None, num_gpus=None)


def get_http_udf_options() -> UDFOptions:
    """Get UDF options for HTTP-based providers."""
    runner = get_or_infer_runner_type()

    if runner == "native":
        # For native runner, use 1 concurrency
        return UDFOptions(concurrency=1, num_gpus=None)
    elif runner == "ray":
        # For ray runner, use number of nodes as concurrency
        import ray

        num_nodes = len(
            [
                node
                for node in ray.nodes()
                if "Resources" in node and "CPU" in node["Resources"] and node["Resources"]["CPU"] > 0
            ]
        )

        if num_nodes > 0:
            return UDFOptions(concurrency=num_nodes, num_gpus=None)
        else:
            # If there are no nodes it means that there are no workers available yet.
            # So we set concurrency to 1 as a fallback.
            # TODO: We should enable autoscaling UDFs in the future.
            return UDFOptions(concurrency=1, num_gpus=None)
    else:
        raise ValueError(f"Invalid runner type: {runner}, expected 'native' or 'ray'")
