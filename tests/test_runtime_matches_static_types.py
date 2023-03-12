"""Tests to ensure that table schemas match with table types at runtime. Daft needs to make sure that the following
invariants are ALWAYS maintained:

A kernel `f(s1, s2, ..., sn)` throws an error IF-AND-ONLY-IF `typecheck(s1.dtype, s2.dtype, ..., sn.dtype)` throws an error

This means that users will see these errors at schema resolving-time and never at runtime.

To do so, we do:

for kernel in KERNELS:
    for dtype_permutation in all_dtype_permutations(kernel.num_args):
        if valid(kernel, dtype_permutation):
            kernel(*[dtype[dt] for dt in dtype_permutation])
        else:
            with pytest.raises(...):
"""


from __future__ import annotations

import dataclasses
import itertools
import operator as ops
from typing import Callable

import numpy as np
import pytest

from daft.datatype import DataType
from daft.expressions2 import ExpressionsProjection, col
from daft.table import Table

# For each datatype, we will generate an array pre-filled with the values in this dictionary
# Note that this does not do corner-case testing. We leave runtime corner-case testing to each individual kernel implementation's unit tests.

ALL_DTYPES = {
    DataType.int8(): np.array([1] * 3, dtype=np.int8),
    DataType.int16(): np.array([1] * 3, dtype=np.int16),
    DataType.int32(): np.array([1] * 3, dtype=np.int32),
    DataType.int64(): np.array([1] * 3, dtype=np.int64),
    DataType.uint8(): np.array([1] * 3, dtype=np.uint8),
    DataType.uint16(): np.array([1] * 3, dtype=np.uint16),
    DataType.uint32(): np.array([1] * 3, dtype=np.uint32),
    DataType.uint64(): np.array([1] * 3, dtype=np.uint64),
    DataType.float32(): np.array([1] * 3, dtype=np.float32),
    DataType.float64(): np.array([1] * 3, dtype=np.float64),
    DataType.string(): np.array(["foo"] * 3, dtype=np.object_),
    # TODO: [RUST-INT][TPCH] Activate tests once these types have been implemented
    # DataType.date(): np.array([datetime.date(2021, 1, 1)] * 3, dtype=np.object_),
    # DataType.bool(): np.array([True] * 3, dtype=np.bool),
    # DataType.null(): np.array([None] * 3, dtype=np.object_),
    # TODO: [RUST-INT] Implement tests for these types
    # DataType.binary(): np.array([b"foo"] * 3, dtype=np.object_),
}


@dataclasses.dataclass(frozen=True)
class Kernel:
    name: str
    num_args: int
    # Callable that takes `num_args` number of arguments, each of which is an expression
    func: Callable


ALL_KERNELS = [
    Kernel(name="add", num_args=2, func=ops.add),
    Kernel(name="sub", num_args=2, func=ops.sub),
    Kernel(name="mul", num_args=2, func=ops.mul),
    Kernel(name="truediv", num_args=2, func=ops.mod),
    Kernel(name="mod", num_args=2, func=ops.truediv),
    Kernel(name="and", num_args=2, func=ops.and_),
    Kernel(name="or", num_args=2, func=ops.or_),
    Kernel(name="lt", num_args=2, func=ops.lt),
    Kernel(name="le", num_args=2, func=ops.le),
    Kernel(name="eq", num_args=2, func=ops.eq),
    Kernel(name="ne", num_args=2, func=ops.ne),
    Kernel(name="ge", num_args=2, func=ops.ge),
    Kernel(name="gt", num_args=2, func=ops.gt),
    Kernel(name="alias", num_args=1, func=lambda e: e.alias("foo")),
    # TODO: How to parametrize over the possible args to this?
    # Kernel(name="cast", num_args=2, func=ops.gt),
]


TEST_PARAMS = [
    pytest.param(kernel, dtype_permutation, id=f"{kernel.name}:{'-'.join([repr(dt) for dt in dtype_permutation])}")
    for kernel in ALL_KERNELS
    for dtype_permutation in itertools.product(ALL_DTYPES.keys(), repeat=kernel.num_args)
]


@pytest.mark.parametrize(["kernel", "dtypes"], TEST_PARAMS)
def test_schema_resolve_validation_matches_runtime_behavior(kernel: Kernel, dtypes: tuple[DataType, ...]):
    assert kernel.num_args == len(dtypes), "Test harness must pass in the same number of dtypes as kernel.num_args"

    table = Table.from_pydict({f"col_{i}": ALL_DTYPES[dt] for i, dt in enumerate(dtypes)})

    projection = ExpressionsProjection(
        [kernel.func(*[col(f"col_{i}") for i in range(kernel.num_args)]).alias("col_result")]
    )

    # Try to resolve the schema, or keep as None if an error occurs
    resolved_schema = None
    try:
        resolved_schema = projection.resolve_schema(table.schema())
    except ValueError:
        pass

    # If an error occurs during schema resolution, assert that an error would occur at runtime as well
    if resolved_schema is None:
        with pytest.raises(ValueError):
            table.eval_expression_list(projection)
    # Otherwise, assert that kernel works at runtime, and check the dtype of the resulting data
    else:
        result_table = table.eval_expression_list(projection)
        result_series = result_table.get_column("col_result")
        assert result_series.datatype() == resolved_schema["col_result"].dtype
