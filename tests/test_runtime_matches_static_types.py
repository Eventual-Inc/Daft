from __future__ import annotations

import dataclasses
import itertools
import operator as ops
from typing import Callable

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.expressions2 import Expression, ExpressionsProjection, col
from daft.series import Series
from daft.table import Table

# For each datatype, we will generate an array pre-filled with the values in this dictionary
# Note that this does not do corner-case testing. We leave runtime corner-case testing to each individual kernel implementation's unit tests.

ALL_DTYPES = [
    DataType.int8(),
    DataType.int16(),
    DataType.int32(),
    DataType.int64(),
    DataType.uint8(),
    DataType.uint16(),
    DataType.uint32(),
    DataType.uint64(),
    DataType.float32(),
    DataType.float64(),
    DataType.string(),
    # TODO: [RUST-INT][TPCH] Activate tests once these types have been implemented
    # DataType.date(),
    # DataType.bool(),
    # DataType.null(),
    # TODO: [RUST-INT] Implement tests for these types
    # DataType.binary(),
]


@dataclasses.dataclass(frozen=True)
class KernelSpec:
    name: str
    num_args: int
    # Callable that takes `num_args` number of arguments, each of which is EITHER an expression or Series
    func: Callable
    # Optional list of non-Expression kwargs that the kernel should be tested against
    kwarg_variants: list[dict] | None = None


def _cast(e: Expression | Series, cast_to: DataType) -> Expression | Series:
    return e.cast(cast_to)


ALL_KERNELS = [
    KernelSpec(name="add", num_args=2, func=ops.add),
    KernelSpec(name="sub", num_args=2, func=ops.sub),
    KernelSpec(name="mul", num_args=2, func=ops.mul),
    KernelSpec(name="truediv", num_args=2, func=ops.mod),
    KernelSpec(name="mod", num_args=2, func=ops.truediv),
    KernelSpec(name="and", num_args=2, func=ops.and_),
    KernelSpec(name="or", num_args=2, func=ops.or_),
    KernelSpec(name="lt", num_args=2, func=ops.lt),
    KernelSpec(name="le", num_args=2, func=ops.le),
    KernelSpec(name="eq", num_args=2, func=ops.eq),
    KernelSpec(name="ne", num_args=2, func=ops.ne),
    KernelSpec(name="ge", num_args=2, func=ops.ge),
    KernelSpec(name="gt", num_args=2, func=ops.gt),
    # KernelSpec(name="alias", num_args=1, func=lambda e: e.alias("foo")),
    KernelSpec(name="cast", num_args=1, func=_cast, kwarg_variants=[{"cast_to": dt for dt in ALL_DTYPES}]),
    # TODO: [RUST-INT][TPCH] Activate tests once these kernels have been implemented
    # KernelSpec(name="sum", num_args=1, func=lambda e: e.agg.sum()),
]


# Generate parameters: for every kernel, for every possible type permutation, for every possible kwarg variant
TEST_PARAMS = [
    pytest.param(
        kernel,
        dtype_permutation,
        kernel_func_kwargs,
        id=f"{kernel.name}:{'-'.join([repr(dt) for dt in dtype_permutation])}",
    )
    for kernel in ALL_KERNELS
    for dtype_permutation in itertools.product(ALL_DTYPES, repeat=kernel.num_args)
    for kernel_func_kwargs in (kernel.kwarg_variants if kernel.kwarg_variants is not None else [{}])
]


@pytest.mark.parametrize(["kernel", "dtypes", "kernel_func_kwargs"], TEST_PARAMS)
def test_schema_resolve_validation_matches_runtime_behavior(
    kernel: KernelSpec, dtypes: tuple[DataType, ...], kernel_func_kwargs: dict
):
    """Test to ensure that table schemas match with table types at runtime.

    To ensure that users will always see type errors at schema resolving-time and not at runtime, Daft needs ensure the following invariant:

    A kernel `f(s1, s2, ..., sn)` throws an error IF-AND-ONLY-IF `typecheck(s1.dtype, s2.dtype, ..., sn.dtype)` throws an error
    """
    assert kernel.num_args == len(dtypes), "Test harness must pass in the same number of dtypes as kernel.num_args"

    data = {f"col_{i}": Series.from_arrow(pa.array([1, 2, 3])) for i in range(len(dtypes))}
    table = Table.from_pydict({f"col_{i}": [1, 2, 3] for i in range(len(dtypes))})
    table = table.eval_expression_list(ExpressionsProjection([col(f"col_{i}").cast(dt) for i, dt in enumerate(dtypes)]))
    projection = ExpressionsProjection(
        [kernel.func(*[col(f"col_{i}") for i in range(kernel.num_args)], **kernel_func_kwargs).alias("col_result")]
    )

    # Try to resolve the schema, or keep as None if an error occurs
    resolved_schema = None
    try:
        resolved_schema = projection.resolve_schema(table.schema())
    except ValueError:
        pass

    def run_kernel():
        return kernel.func(*[data[f"col_{i}"].cast(dt) for i, dt in enumerate(dtypes)], **kernel_func_kwargs)

    # If an error occurs during schema resolution, assert that a ValueError would occur at runtime as well
    if resolved_schema is None:
        with pytest.raises(ValueError):
            run_kernel()
    # Otherwise, assert that kernel works at runtime, and check the dtype of the resulting data
    else:
        result_series = run_kernel()
        assert result_series.datatype() == resolved_schema["col_result"].dtype
