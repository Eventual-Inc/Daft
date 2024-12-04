from __future__ import annotations

import datetime
import decimal

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft import DataType, Series, TimeUnit

PYARROW_GE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (8, 0, 0)


@pytest.mark.skipif(
    not PYARROW_GE_8_0_0,
    reason="PyArrow writing to Parquet does not have good coverage for all types for versions <8.0.0",
)
@pytest.mark.parametrize(
    ["data", "pa_type", "expected_dtype"],
    [
        ([1, 2, None], pa.int64(), DataType.int64()),
        (["a", "b", None], pa.large_string(), DataType.string()),
        ([True, False, None], pa.bool_(), DataType.bool()),
        ([b"a", b"b", None], pa.large_binary(), DataType.binary()),
        ([None, None, None], pa.null(), DataType.null()),
        ([decimal.Decimal("1.23"), decimal.Decimal("1.24"), None], pa.decimal128(16, 8), DataType.decimal128(16, 8)),
        ([datetime.date(1994, 1, 1), datetime.date(1995, 1, 1), None], pa.date32(), DataType.date()),
        (
            [datetime.time(12, 1, 22, 4), datetime.time(13, 8, 45, 34), None],
            pa.time64("us"),
            DataType.time(TimeUnit.us()),
        ),
        (
            [datetime.time(12, 1, 22, 4), datetime.time(13, 8, 45, 34), None],
            pa.time64("ns"),
            DataType.time(TimeUnit.ns()),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms"),
            DataType.timestamp(TimeUnit.ms()),
        ),
        ([datetime.date(1994, 1, 1), datetime.date(1995, 1, 1), None], pa.date64(), DataType.timestamp(TimeUnit.ms())),
        (
            [datetime.timedelta(days=1), datetime.timedelta(days=2), None],
            pa.duration("ms"),
            DataType.duration(TimeUnit.ms()),
        ),
        ([[1, 2, 3], [], None], pa.large_list(pa.int64()), DataType.list(DataType.int64())),
        # TODO: Crashes when parsing fixed size lists
        # ([[1, 2, 3], [4, 5, 6], None], pa.list_(pa.int64(), list_size=3), DataType.fixed_size_list(DataType.int64(), 3)),
        ([{"bar": 1}, {"bar": None}, None], pa.struct({"bar": pa.int64()}), DataType.struct({"bar": DataType.int64()})),
        (
            [[("a", 1), ("b", 2)], [], None],
            pa.map_(pa.large_string(), pa.int64()),
            DataType.map(DataType.string(), DataType.int64()),
        ),
    ],
)
def test_roundtrip_simple_arrow_types(tmp_path, data, pa_type, expected_dtype):
    before = daft.from_arrow(pa.table({"foo": pa.array(data, type=pa_type)}))
    before = before.concat(before)
    before.write_parquet(str(tmp_path))
    after = daft.read_parquet(str(tmp_path))
    assert before.schema()["foo"].dtype == expected_dtype
    assert after.schema()["foo"].dtype == expected_dtype
    assert before.to_arrow() == after.to_arrow()


@pytest.mark.parametrize(
    ["data", "pa_type", "expected_dtype"],
    [
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", None),
            DataType.timestamp(TimeUnit.ms(), None),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", "+00:00"),
            DataType.timestamp(TimeUnit.ms(), "+00:00"),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", "UTC"),
            DataType.timestamp(TimeUnit.ms(), "UTC"),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", "+08:00"),
            DataType.timestamp(TimeUnit.ms(), "+08:00"),
        ),
    ],
)
def test_roundtrip_temporal_arrow_types(tmp_path, data, pa_type, expected_dtype):
    before = daft.from_arrow(pa.table({"foo": pa.array(data, type=pa_type)}))
    before = before.concat(before)
    before.write_parquet(str(tmp_path))
    after = daft.read_parquet(str(tmp_path))
    assert before.schema()["foo"].dtype == expected_dtype
    assert after.schema()["foo"].dtype == expected_dtype
    assert before.to_arrow() == after.to_arrow()


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


# TODO: reading/writing:
# 1. Embedding type
# 2. Image type
# 3. Extension type?
