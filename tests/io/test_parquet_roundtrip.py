from __future__ import annotations

import random

import numpy as np
import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft import DataType, Series

PYARROW_GE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (8, 0, 0)


def test_roundtrip_tensor_types(tmp_path):
    # Define the expected data type for the tensor column
    expected_tensor_dtype = DataType.tensor(DataType.int64())

    # Create sample tensor data with some null values
    tensor_data = [np.array([[1, 2], [3, 4]]), None, None]

    # Create a Daft DataFrame with the tensor data
    df_original = daft.from_pydict({"tensor_col": Series.from_pylist(tensor_data)})

    # Double the size of the DataFrame to ensure we test with more data
    df_original = df_original.concat(df_original)

    assert df_original.schema()["tensor_col"].dtype == expected_tensor_dtype

    # Write the DataFrame to a Parquet file
    df_original.write_parquet(str(tmp_path))

    # Read the Parquet file back into a new DataFrame
    df_roundtrip = daft.read_parquet(str(tmp_path))

    # Verify that the data type is preserved after the roundtrip
    assert df_roundtrip.schema()["tensor_col"].dtype == expected_tensor_dtype

    # Ensure the data content is identical after the roundtrip
    assert df_original.to_arrow() == df_roundtrip.to_arrow()


@pytest.mark.parametrize("fixed_shape", [True, False])
def test_roundtrip_sparse_tensor_types(tmp_path, fixed_shape):
    if fixed_shape:
        expected_dtype = DataType.sparse_tensor(DataType.int64(), (2, 2))
        data = [np.array([[0, 0], [1, 0]]), None, np.array([[0, 0], [0, 0]]), np.array([[0, 1], [0, 0]])]
    else:
        expected_dtype = DataType.sparse_tensor(DataType.int64())
        data = [np.array([[0, 0], [1, 0]]), None, np.array([[0, 0]]), np.array([[0, 1, 0], [0, 0, 1]])]
    before = daft.from_pydict({"foo": Series.from_pylist(data)})
    before = before.with_column("foo", before["foo"].cast(expected_dtype))
    before = before.concat(before)
    before.write_parquet(str(tmp_path))
    after = daft.read_parquet(str(tmp_path))
    assert before.schema()["foo"].dtype == expected_dtype
    assert after.schema()["foo"].dtype == expected_dtype
    assert before.to_arrow() == after.to_arrow()


@pytest.mark.parametrize("has_none", [True, False])
def test_roundtrip_boolean_rle(tmp_path, has_none):
    file_path = f"{tmp_path}/test.parquet"
    if has_none:
        # Create an array of random True/False values that are None 10% of the time.
        random_bools = random.choices([True, False, None], weights=[45, 45, 10], k=1000_000)
    else:
        # Create an array of random True/False values.
        random_bools = random.choices([True, False], k=1000_000)
    pa_original = pa.table({"bools": pa.array(random_bools, type=pa.bool_())})
    # Use data page version 2.0 which uses RLE encoding for booleans.
    papq.write_table(pa_original, file_path, data_page_version="2.0")
    df_roundtrip = daft.read_parquet(file_path)
    assert pa_original == df_roundtrip.to_arrow()


# TODO: reading/writing:

# 2. Image type
# 3. Extension type?
