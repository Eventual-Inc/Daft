from __future__ import annotations

import dataclasses
import sys
import time
from typing import TYPE_CHECKING, Any, Callable

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

from daft.datatype import DataType
from daft.dependencies import np, torch
from daft.expressions import Expression
from daft.series import Series

if TYPE_CHECKING:
    from torch._prims_common import DeviceLikeType

    from daft.daft import PyDataType

    GPUTorchFunc: TypeAlias = Callable[..., torch.Tensor]


class StreamManager:
    def __init__(self, device: DeviceLikeType, inner: GPUTorchFunc):
        self.device = device
        self.inner = inner
        self.stream = torch.cuda.Stream(self.device)
        self.start_event = torch.cuda.Event(enable_timing=True)
        self.h2d_event = torch.cuda.Event(enable_timing=True)
        self.func_event = torch.cuda.Event(enable_timing=True)

        self.output_tensor = None

    def start(self, init_arg: Any, input: Series) -> None:
        """Process a single batch on GPU stream with timing."""
        # Convert to Torch tensor
        input_batch = torch.as_tensor(np.array(input.to_pylist()))

        # Start GPU work on this stream
        with torch.cuda.stream(self.stream):
            # Record start
            self.start_event.record(self.stream)

            # Move data to GPU
            batch_gpu = input_batch.to(device=self.device, non_blocking=True)
            self.h2d_event.record(self.stream)

            # Run GPU UDF
            output = self.inner(init_arg, batch_gpu)
            self.func_event.record(self.stream)

        self.output_tensor = output

    def get_output(self, out_dtype: PyDataType) -> tuple[torch.Tensor, float, float, float] | None:
        if not self.stream.query():
            return None

        assert self.output_tensor is not None

        # Move output back to CPU and convert to Numpy
        start = time.time()
        output_cpu = self.output_tensor.cpu()
        output = Series.from_numpy(np.array(output_cpu), dtype=DataType._from_pydatatype(out_dtype))
        d2h_time = time.time() - start

        self.output_tensor = None

        return (
            output._series,
            self.h2d_event.elapsed_time(self.start_event),
            self.func_event.elapsed_time(self.h2d_event),
            d2h_time * 1000.0,
        )


@dataclasses.dataclass
class GpuUdf:
    inner: GPUTorchFunc
    name: str
    return_daft_dtype: DataType
    device: DeviceLikeType
    init_fn: Callable[[], Any]
    batch_size: int
    num_streams: int | None

    def __call__(self, arg: Expression) -> Expression:
        return Expression._gpu_udf(
            arg=arg,
            name=self.name,
            inner=self.inner,
            return_dtype=self.return_daft_dtype,
            device=self.device,
            init_fn=self.init_fn,
            batch_size=self.batch_size,
            num_streams=self.num_streams,
        )


def _nothing() -> None:
    return


def gpu_udf(
    *,
    return_dtype: DataType,
    device: DeviceLikeType,
    init_fn: Callable[[], Any] = _nothing,
    batch_size: int = 64,
    num_streams: int | None = None,
) -> Callable[[GPUTorchFunc], GpuUdf]:
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
            return_dtype,
            device,
            init_fn,
            batch_size,
            num_streams,
        )

        return udf

    return _udf
