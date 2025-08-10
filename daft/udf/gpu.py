from __future__ import annotations

import dataclasses
import sys
from typing import TYPE_CHECKING, Any, Callable

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

from daft.datatype import DataType
from daft.expressions import Expression

if TYPE_CHECKING:
    import torch
    from torch._prims_common import DeviceLikeType

    GPUTorchFunc: TypeAlias = Callable[..., torch.Tensor]


def run_gpu(
    func: GPUTorchFunc,
    stream: torch.cuda.Stream,
    device: DeviceLikeType,
    init_arg: Any,
    input_batch: torch.Tensor,
) -> tuple:
    """Process a single batch on GPU stream with timing"""
    # Create CUDA events for timing
    start_event = torch.cuda.Event(enable_timing=True)
    h2d_event = torch.cuda.Event(enable_timing=True)  # host to device
    func_event = torch.cuda.Event(enable_timing=True)
    d2h_event = torch.cuda.Event(enable_timing=True)  # device to host

    # Start GPU work on this stream
    with torch.cuda.stream(stream):
        # Record start
        start_event.record(stream)

        # Move data to GPU
        batch_gpu = input_batch.to(device=device, non_blocking=True)
        h2d_event.record(stream)

        # Run GPU UDF
        output = func(init_arg, batch_gpu)
        func_event.record(stream)

        # Move result back to CPU
        result = output.cpu()
        d2h_event.record(stream)

    return (result, start_event, h2d_event, func_event, d2h_event)


def gen_event_times(
    start_event: torch.cuda.Event,
    h2d_event: torch.cuda.Event,
    func_event: torch.cuda.Event,
    d2h_event: torch.cuda.Event,
) -> tuple[float, float, float, float]:
    h2d_time = h2d_event.elapsed_time(start_event)
    inference_time = func_event.elapsed_time(h2d_event)
    d2h_time = d2h_event.elapsed_time(func_event)
    total_time = d2h_event.elapsed_time(start_event)

    return h2d_time, inference_time, d2h_time, total_time


@dataclasses.dataclass
class GpuUdf:
    inner: GPUTorchFunc
    name: str
    return_torch_dtype: torch.dtype
    return_torch_row_size: torch.Size
    return_daft_dtype: DataType
    init_args: Callable[[], Any]
    device: DeviceLikeType
    gpu_mem: float

    def __call__(self, arg: Expression) -> Expression:
        return Expression._gpu_udf(
            arg=arg,
            name=self.name,
            inner=self.inner,
            init_args=self.init_args,
            device=self.device,
            gpu_mem=self.gpu_mem,
        )


def _nothing() -> None:
    return


def gpu_udf(
    *,
    return_torch_dtype: torch.dtype,
    return_torch_row_size: torch.Size,
    device: DeviceLikeType,
    init_args: Callable[[], Any] = _nothing,
    gpu_mem: float = 1.0,
) -> Callable[[GPUTorchFunc], GpuUdf]:
    """TODO"""
    if return_torch_dtype == torch.float32:
        ret_inner_dtype = DataType.float32()
    elif return_torch_dtype == torch.float64:
        ret_inner_dtype = DataType.float64()
    else:
        raise ValueError(f"Unsupported torch dtype: {return_torch_dtype}")

    if len(return_torch_row_size) == 1:
        ret_dtype = DataType.embedding(ret_inner_dtype, return_torch_row_size[0])
    else:
        ret_dtype = DataType.tensor(ret_inner_dtype, tuple(return_torch_row_size))

    def _udf(f: GPUTorchFunc) -> GpuUdf:
        # Grab a name for the UDF. It **should** be unique.
        module_name = getattr(f, "__module__", "")
        qual_name = getattr(f, "__qualname__")

        if module_name:
            name = f"{module_name}.{qual_name}"
        else:
            name = qual_name

        udf = GpuUdf(
            f,
            name,
            return_torch_dtype,
            return_torch_row_size,
            ret_dtype,
            device,
            init_args,
            gpu_mem,
        )

        return udf

    return _udf
