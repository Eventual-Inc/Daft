from __future__ import annotations

import math
import pickle

import pytest

import daft
from daft import DataType, col

# ============ Type Construction Tests ============


def test_bfloat16_type_construction():
    dtype = DataType.bfloat16()
    assert str(dtype) == "BFloat16"


def test_bfloat16_type_equality():
    assert DataType.bfloat16() == DataType.bfloat16()
    assert DataType.bfloat16() != DataType.float32()


def test_bfloat16_pickle_roundtrip():
    dtype = DataType.bfloat16()
    assert pickle.loads(pickle.dumps(dtype)) == dtype


# ============ DataFrame Integration Tests ============


def test_bfloat16_cast_from_float32():
    df = daft.from_pydict({"x": [1.0, 2.0, 3.0]})
    df = df.with_column("bf16", col("x").cast(DataType.bfloat16()))
    result = df.collect()
    assert result.schema()["bf16"].dtype == DataType.bfloat16()


def test_bfloat16_cast_to_float32():
    df = daft.from_pydict({"x": [1.0, 2.0, 3.0]})
    df = df.with_column("bf16", col("x").cast(DataType.bfloat16()))
    df = df.with_column("f32", col("bf16").cast(DataType.float32()))
    result = df.collect().to_pydict()
    # Values should round-trip (these values are exactly representable in bf16)
    assert result["f32"] == [1.0, 2.0, 3.0]


def test_bfloat16_cast_roundtrip_precision():
    # 1.099609375 is NOT exactly representable in bf16 (1.09375 is nearest)
    df = daft.from_pydict({"x": [1.099609375]})
    df = df.with_column("bf16", col("x").cast(DataType.bfloat16()))
    df = df.with_column("f32", col("bf16").cast(DataType.float32()))
    result = df.collect().to_pydict()
    # Should be truncated to bf16 precision
    assert result["f32"][0] != 1.099609375  # precision lost
    assert abs(result["f32"][0] - 1.099609375) < 0.02  # but close


def test_bfloat16_with_nulls():
    df = daft.from_pydict({"x": [1.0, None, 3.0]})
    df = df.with_column("bf16", col("x").cast(DataType.bfloat16()))
    df = df.with_column("f32", col("bf16").cast(DataType.float32()))
    result = df.collect().to_pydict()
    assert result["f32"][0] == 1.0
    assert result["f32"][1] is None
    assert result["f32"][2] == 3.0


def test_bfloat16_cast_to_utf8():
    df = daft.from_pydict({"x": [1.0, 2.5, 3.0]})
    df = df.with_column("bf16", col("x").cast(DataType.bfloat16()))
    df = df.with_column("s", col("bf16").cast(DataType.string()))
    result = df.collect().to_pydict()
    # Should produce string representations
    assert all(isinstance(s, str) for s in result["s"])


def test_bfloat16_cast_from_int():
    df = daft.from_pydict({"x": [1, 2, 3]})
    df = df.with_column("bf16", col("x").cast(DataType.bfloat16()))
    df = df.with_column("f32", col("bf16").cast(DataType.float32()))
    result = df.collect().to_pydict()
    assert result["f32"] == [1.0, 2.0, 3.0]


# ============ ml_dtypes Interop Tests ============


def test_from_ml_dtypes_numpy_array():
    ml_dtypes = pytest.importorskip("ml_dtypes")
    import numpy as np

    arr = np.array([1.0, 2.0, 3.0], dtype=ml_dtypes.bfloat16)
    df = daft.from_pydict({"col": arr})
    assert df.schema()["col"].dtype == DataType.bfloat16()


def test_ml_dtypes_values_roundtrip():
    ml_dtypes = pytest.importorskip("ml_dtypes")
    import numpy as np

    arr = np.array([1.0, 2.5, -3.0, 0.0], dtype=ml_dtypes.bfloat16)
    df = daft.from_pydict({"col": arr})
    df = df.with_column("f32", col("col").cast(DataType.float32()))
    result = df.collect().to_pydict()
    assert result["f32"] == [1.0, 2.5, -3.0, 0.0]


def test_ml_dtypes_special_values():
    ml_dtypes = pytest.importorskip("ml_dtypes")
    import numpy as np

    arr = np.array([float("nan"), float("inf"), float("-inf"), 0.0], dtype=ml_dtypes.bfloat16)
    df = daft.from_pydict({"col": arr})
    df = df.with_column("f32", col("col").cast(DataType.float32()))
    result = df.collect().to_pydict()
    assert math.isnan(result["f32"][0])
    assert result["f32"][1] == float("inf")
    assert result["f32"][2] == float("-inf")
    assert result["f32"][3] == 0.0


# ============ PyTorch Interop Tests ============


class TestBFloat16Torch:
    @pytest.fixture(autouse=True)
    def _skip_no_torch(self):
        pytest.importorskip("torch")

    def test_from_torch_bfloat16_list(self):
        import torch

        # Create bf16 tensor, convert to list of Python floats
        t = torch.tensor([1.0, 2.0, 3.0], dtype=torch.bfloat16)
        values = t.tolist()
        df = daft.from_pydict({"col": values})
        df = df.with_column("bf16", col("col").cast(DataType.bfloat16()))
        result = df.collect()
        assert result.schema()["bf16"].dtype == DataType.bfloat16()


# ============ Arithmetic Type Rules Tests ============


def test_bfloat16_add_bfloat16():
    df = daft.from_pydict({"a": [1.0, 2.0], "b": [3.0, 4.0]})
    df = df.with_column("a_bf16", col("a").cast(DataType.bfloat16()))
    df = df.with_column("b_bf16", col("b").cast(DataType.bfloat16()))
    df = df.with_column("sum", col("a_bf16") + col("b_bf16"))
    result = df.collect()
    # bf16 + bf16 should return bf16
    assert result.schema()["sum"].dtype == DataType.bfloat16()


def test_bfloat16_add_float32():
    df = daft.from_pydict({"a": [1.0], "b": [2.0]})
    df = df.with_column("a_bf16", col("a").cast(DataType.bfloat16()))
    df = df.with_column("b_f32", col("b").cast(DataType.float32()))
    df = df.with_column("sum", col("a_bf16") + col("b_f32"))
    result = df.collect()
    # bf16 + f32 should promote to f32
    assert result.schema()["sum"].dtype == DataType.float32()
