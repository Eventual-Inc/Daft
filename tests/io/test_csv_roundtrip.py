from __future__ import annotations

import datetime
import decimal

import pyarrow as pa
import pytest

import daft
from daft import DataType, TimeUnit

PYARROW_GE_11_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (11, 0, 0)


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
@pytest.mark.parametrize(
    ["data", "pa_type", "expected_dtype", "expected_inferred_dtype"],
    [
        ([1, 2, None], pa.int64(), DataType.int64(), DataType.int64()),
        (["a", "b", ""], pa.large_string(), DataType.string(), DataType.string()),
        ([b"a", b"b", b""], pa.large_binary(), DataType.binary(), DataType.string()),
        ([True, False, None], pa.bool_(), DataType.bool(), DataType.bool()),
        ([None, None, None], pa.null(), DataType.null(), DataType.null()),
        (
            [decimal.Decimal("1.23"), decimal.Decimal("1.24"), None],
            pa.decimal128(16, 8),
            DataType.decimal128(16, 8),
            DataType.float64(),
        ),
        ([datetime.date(1994, 1, 1), datetime.date(1995, 1, 1), None], pa.date32(), DataType.date(), DataType.date()),
        (
            [datetime.time(1, 2, 3, 4), datetime.time(5, 6, 7, 8), None],
            pa.time64("us"),
            DataType.time(TimeUnit.us()),
            DataType.time(TimeUnit.us()),
        ),
        (
            [datetime.time(1, 2, 3, 4), datetime.time(5, 6, 7, 8), None],
            pa.time64("ns"),
            DataType.time(TimeUnit.ns()),
            DataType.time(TimeUnit.us()),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms"),
            DataType.timestamp(TimeUnit.ms()),
            # NOTE: Seems like the inferred type is seconds because it's written with seconds resolution
            DataType.timestamp(TimeUnit.s()),
        ),
        (
            [datetime.date(1994, 1, 1), datetime.date(1995, 1, 1), None],
            pa.date64(),
            DataType.timestamp(TimeUnit.ms()),
            DataType.timestamp(TimeUnit.s()),
        ),
        (
            [datetime.timedelta(days=1), datetime.timedelta(days=2), None],
            pa.duration("ms"),
            DataType.duration(TimeUnit.ms()),
            # NOTE: Duration ends up being written as int64
            DataType.int64(),
        ),
        # TODO: Verify that these types throw an error when we write dataframes with them
        # ([[1, 2, 3], [], None], pa.large_list(pa.int64()), DataType.list(DataType.int64())),
        # ([[1, 2, 3], [4, 5, 6], None], pa.list_(pa.int64(), list_size=3), DataType.fixed_size_list(DataType.int64(), 3)),
        # ([{"bar": 1}, {"bar": None}, None], pa.struct({"bar": pa.int64()}), DataType.struct({"bar": DataType.int64()})),
    ],
)
def test_roundtrip_simple_arrow_types(tmp_path, data, pa_type, expected_dtype, expected_inferred_dtype):
    before = daft.from_arrow(pa.table({"id": pa.array(range(3)), "foo": pa.array(data, type=pa_type)}))
    before = before.concat(before)
    before.write_csv(str(tmp_path))
    after = daft.read_csv(str(tmp_path))
    assert before.schema()["foo"].dtype == expected_dtype
    assert after.schema()["foo"].dtype == expected_inferred_dtype
    assert before.to_arrow() == after.with_column("foo", after["foo"].cast(expected_dtype)).to_arrow()
