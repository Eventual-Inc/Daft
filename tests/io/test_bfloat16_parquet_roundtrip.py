from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft import DataType, Series

PYARROW_GE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (8, 0, 0)


@pytest.mark.skipif(
    not PYARROW_GE_8_0_0,
    reason="PyArrow <8.0.0 has limited Parquet coverage for complex types",
)
def test_roundtrip_bfloat16_tensor_parquet(tmp_path) -> None:
    """Parquet round-trip for a Tensor[BFloat16] column.

    This verifies that a tensor column with logical dtype Tensor[BFloat16] can be
    written to and read back from Parquet via the Arrow extension type
    "daft.bfloat16", while using Float32 as the physical storage type.
    """

    torch = pytest.importorskip("torch")
    if not hasattr(torch, "bfloat16"):
        pytest.skip("torch.bfloat16 is not available")

    # Construct a small column of BF16 tensors, including a None value.
    tensor_data = [torch.randn(2, 2, dtype=torch.bfloat16), None]

    s = Series.from_pylist(
        tensor_data,
        dtype=DataType.tensor(DataType.bfloat16()),
        pyobj="allow",
    )

    expected_dtype = DataType.tensor(DataType.bfloat16())

    df_before = daft.from_pydict({"tensor_col": s})
    assert df_before.schema()["tensor_col"].dtype == expected_dtype

    # Duplicate rows to exercise a slightly larger round-trip.
    df_before = df_before.concat(df_before)

    df_before.write_parquet(str(tmp_path))
    df_roundtrip = daft.read_parquet(str(tmp_path))

    # Logical dtype should be preserved as Tensor[BFloat16].
    assert df_roundtrip.schema()["tensor_col"].dtype == expected_dtype

    out_after = df_roundtrip.to_pydict()["tensor_col"]
    assert len(out_after) == 4

    # Each non-null element should be materializable as a NumPy array that can
    # be safely upcast to float32, reflecting the logical BF16 + physical F32
    # storage strategy.
    for arr in out_after:
        if arr is None:
            continue
        
        if isinstance(arr, torch.Tensor):
            assert arr.dtype == torch.bfloat16
            arr = arr.float().numpy()

        assert isinstance(arr, np.ndarray)
        np.asarray(arr, dtype=np.float32)
