from __future__ import annotations

import datetime
import decimal
import random
import uuid

import numpy as np
import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft import DataType, Series, TimeUnit
from daft.context import execution_config_ctx


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
@pytest.mark.parametrize("native_parquet_writer", [True, False])
def test_roundtrip_simple_arrow_types(tmp_path, data, pa_type, expected_dtype, native_parquet_writer):
    before = daft.from_arrow(pa.table({"foo": pa.array(data, type=pa_type)}))
    before = before.concat(before)

    with execution_config_ctx(native_parquet_writer=native_parquet_writer):
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
@pytest.mark.parametrize("native_parquet_writer", [True, False])
def test_roundtrip_temporal_arrow_types(tmp_path, data, pa_type, expected_dtype, native_parquet_writer: bool):
    """Includes naive and zoned timestamps; native writer preserves tz via ARROW:schema."""
    before = daft.from_arrow(pa.table({"foo": pa.array(data, type=pa_type)}))
    before = before.concat(before)

    with execution_config_ctx(native_parquet_writer=native_parquet_writer):
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


@pytest.mark.skipif(
    not hasattr(pa, "uuid"),
    reason="PyArrow version doesn't support the canonical uuid extension type.",
)
@pytest.mark.parametrize("native_parquet_writer", [True, False])
def test_roundtrip_uuid_type(tmp_path, native_parquet_writer: bool) -> None:
    """Parquet write/read preserves Arrow uuid (logical) and Daft DataType.uuid()."""
    pydict = {"uuid_col": [uuid.uuid4().bytes for _ in range(3)]}
    pa_schema = pa.schema([pa.field("uuid_col", pa.uuid())])
    t = pa.Table.from_pydict(pydict, schema=pa_schema)
    before = daft.from_arrow(t)
    before = before.concat(before)

    with execution_config_ctx(native_parquet_writer=native_parquet_writer):
        before.write_parquet(str(tmp_path))
        after = daft.read_parquet(str(tmp_path))

    assert before.schema()["uuid_col"].dtype == DataType.uuid()
    assert after.schema()["uuid_col"].dtype == DataType.uuid()
    assert before.to_arrow() == after.to_arrow()


@pytest.mark.parametrize("native_parquet_writer", [True, False])
def test_roundtrip_arrow_extension_type(tmp_path, uuid_ext_type, native_parquet_writer: bool) -> None:
    """Parquet write/read preserves a registered Arrow extension column (storage + extension name)."""
    n = 3
    pydict = {
        "id": list(range(n)),
        "ext_col": pa.ExtensionArray.from_storage(uuid_ext_type, pa.array([f"{i}".encode() for i in range(n)])),
    }
    t = pa.Table.from_pydict(pydict)
    before = daft.from_arrow(t)
    expected_dtype = before.schema()["ext_col"].dtype
    assert expected_dtype == DataType.extension(
        uuid_ext_type.NAME, DataType.from_arrow_type(uuid_ext_type.storage_type), ""
    )

    with execution_config_ctx(native_parquet_writer=native_parquet_writer):
        before.write_parquet(str(tmp_path))
        after = daft.read_parquet(str(tmp_path))

    assert before.schema()["ext_col"].dtype == expected_dtype
    assert after.schema()["ext_col"].dtype == expected_dtype
    ba = before.to_arrow()
    aa = after.to_arrow()
    assert ba.equals(aa, check_metadata=False)


# TODO: reading/writing:
# 1. Embedding type
# 2. Image type
