from __future__ import annotations

from typing import TypedDict

import numpy as np
import pytest

import daft
from daft import DataType, col


class BFloat16StructOut(TypedDict):
    len: int
    # The runtime value will be a tensor/ndarray; we keep the annotation loose to
    # avoid importing optional torch at module import time.
    bfloat16: object


def test_row_wise_udf_struct_with_bfloat16_tensor() -> None:
    """Row-wise UDF that returns a Struct containing a BFloat16 tensor.

    This reproduces the scenario of issue #5293, but with an explicit struct
    return dtype so that the resulting column has logical type Struct[len: Int64,
    bfloat16: Tensor[BFloat16]] instead of falling back to Python.
    """

    torch = pytest.importorskip("torch")

    if not hasattr(torch, "bfloat16"):
        pytest.skip("torch.bfloat16 is not available")

    @daft.func(
        return_dtype=DataType.struct(
            {
                "len": DataType.int64(),
                "bfloat16": DataType.tensor(DataType.bfloat16()),
            }
        )
    )
    def bf16_struct_udf(x: "torch.Tensor") -> BFloat16StructOut:  # type: ignore[name-defined]
        assert x.dtype == torch.bfloat16
        return {"len": int(x.shape[0]), "bfloat16": x}

    df = daft.from_pydict({"x": [torch.randn(4, dtype=torch.bfloat16)]})

    df_struct = df.select(bf16_struct_udf(col("x")).alias("s"))

    # The struct column should have the expected logical dtype.
    expected_struct_dtype = DataType.struct(
        {"len": DataType.int64(), "bfloat16": DataType.tensor(DataType.bfloat16())}
    )
    assert df_struct.schema()["s"].dtype == expected_struct_dtype

    # struct.get("bfloat16") should yield a Tensor[BFloat16] column, not Python.
    df_extracted = df_struct.select(
        col("s").get("len").alias("len"),
        col("s").get("bfloat16").alias("bfloat16"),
    )

    assert df_extracted.schema()["len"].dtype == DataType.int64()
    assert df_extracted.schema()["bfloat16"].dtype == DataType.tensor(DataType.bfloat16())

    out = df_extracted.to_pydict()
    assert out["len"] == [4]

    arr = out["bfloat16"][0]
    assert isinstance(arr, (np.ndarray, torch.Tensor))
    if isinstance(arr, torch.Tensor):
        assert arr.dtype == torch.bfloat16
        # Torch BFloat16 cannot be directly converted to numpy; cast to float32 first.
        arr = arr.float()
    assert arr.shape == (4,)
    # Values should be safely representable as float32 (BF16 -> F32 upcast).
    np.asarray(arr, dtype=np.float32)


def test_row_wise_udf_bfloat16_with_jaxtyping() -> None:
    """Row-wise UDF using jaxtyping.BFloat16 annotations.

    This exercises the end-to-end path where a BF16 tensor argument is annotated
    with jaxtyping.BFloat16[...] and the UDF explicitly declares a
    Tensor[BFloat16] return dtype.
    """

    jaxtyping_mod = pytest.importorskip("jaxtyping")
    if not hasattr(jaxtyping_mod, "BFloat16"):
        pytest.skip("jaxtyping.BFloat16 is not available")

    from jaxtyping import BFloat16, jaxtyped  # type: ignore[import]

    torch = pytest.importorskip("torch")
    if not hasattr(torch, "bfloat16"):
        pytest.skip("torch.bfloat16 is not available")

    @jaxtyped
    @daft.func(return_dtype=DataType.tensor(DataType.bfloat16()))
    def my_udf_jax(x: BFloat16[torch.Tensor, "batch dim"]) -> torch.Tensor:
        return x

    df = daft.from_pydict({"x": [torch.randn(2, dtype=torch.bfloat16)]})
    df_out = df.select(my_udf_jax(col("x")).alias("x_bf16"))

    assert df_out.schema()["x_bf16"].dtype == DataType.tensor(DataType.bfloat16())

    out_arr = df_out.to_pydict()["x_bf16"][0]
    assert isinstance(out_arr, (np.ndarray, torch.Tensor))
    if isinstance(out_arr, torch.Tensor):
        assert out_arr.dtype == torch.bfloat16
        out_arr = out_arr.float()
    assert out_arr.shape == (2,)
    # Again, BF16 values should be convertible to float32 without issues.
    np.asarray(out_arr, dtype=np.float32)
