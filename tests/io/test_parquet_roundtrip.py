from __future__ import annotations

import datetime
import decimal

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft import DataType, Series, TimeUnit


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
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms"),
            DataType.timestamp(TimeUnit.ms()),
        ),
        (
            [datetime.timedelta(days=1), datetime.timedelta(days=2), None],
            pa.duration("ms"),
            DataType.duration(TimeUnit.ms()),
        ),
        ([[1, 2, 3], [], None], pa.large_list(pa.int64()), DataType.list(DataType.int64())),
        # TODO: Crashes when parsing fixed size lists
        # ([[1, 2, 3], [4, 5, 6], None], pa.list_(pa.int64(), list_size=3), DataType.fixed_size_list(DataType.int64(), 3)),
        ([{"bar": 1}, {"bar": None}, None], pa.struct({"bar": pa.int64()}), DataType.struct({"bar": DataType.int64()})),
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
        # TODO: Fails and seems to just fall-back onto +00:00
        # ([datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None], pa.timestamp("ms", "UTC"), DataType.timestamp(TimeUnit.ms(), "UTC")),
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


@pytest.mark.skip(reason="Currently fails when reading multiple Parquet files with tensor types")
def test_roundtrip_tensor_types(tmp_path):
    expected_dtype = DataType.tensor(DataType.int64())
    data = [np.array([[1, 2], [3, 4]]), None, None]
    before = daft.from_pydict({"foo": Series.from_pylist(data)})
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
