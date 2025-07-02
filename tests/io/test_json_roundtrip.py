from __future__ import annotations

import datetime
import decimal

import pyarrow as pa
import pytest

import daft
from daft import DataType, TimeUnit
from tests.conftest import get_tests_daft_runner_name

PYARROW_GE_11_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (11, 0, 0)


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
@pytest.mark.parametrize(
    ["data", "pa_type", "expected_dtype", "expected_inferred_dtype"],
    [
        ([1, 2, None], pa.int64(), DataType.int64(), DataType.int64()),
        (["a", "b", ""], pa.large_string(), DataType.string(), DataType.string()),
        # TODO(desmond): Arrow-rs writes binaries as strings of hexadecimals. Technically the JSON spec (RFC 8259) does not
        #                support BINARY. In practice, libraries such as SIMD-JSON expects binary to be represented as a
        #                string that's properly encoded e.g. you can encode your binary values in base64.
        #                We should make our readers and writers compatible.
        # ([b"a", b"b", b""], pa.large_binary(), DataType.binary(), DataType.string()),
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
        # TODO(desmond): Arrow-rs also currently writes durations in ISO 8601 duration format while our reader expects an i64.
        #                We should make our readers and writers compatible.
        # (
        #     [datetime.timedelta(days=1), datetime.timedelta(days=2), None],
        #     pa.duration("ms"),
        #     DataType.duration(TimeUnit.ms()),
        #     # NOTE: Duration ends up being written as int64
        #     DataType.int64(),
        # ),
    ],
)
def test_roundtrip_simple_arrow_types(tmp_path, data, pa_type, expected_dtype, expected_inferred_dtype):
    before = daft.from_arrow(pa.table({"id": pa.array(range(3)), "foo": pa.array(data, type=pa_type)}))
    before = before.concat(before)
    before.write_json(str(tmp_path))
    after = daft.read_json(str(tmp_path))
    assert before.schema()["foo"].dtype == expected_dtype
    assert after.schema()["foo"].dtype == expected_inferred_dtype
    assert before.to_arrow() == after.with_column("foo", after["foo"].cast(expected_dtype)).to_arrow()


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_roundtrip_struct_types(tmp_path):
    struct_data = [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": 25, "city": "San Francisco"},
        None,
    ]

    pa_struct_type = pa.struct({"name": pa.string(), "age": pa.int64(), "city": pa.string()})

    expected_dtype = DataType.struct({"name": DataType.string(), "age": DataType.int64(), "city": DataType.string()})

    before = daft.from_arrow(pa.table({"id": pa.array(range(3)), "person": pa.array(struct_data, type=pa_struct_type)}))
    before = before.concat(before)
    before.write_json(str(tmp_path))
    after = daft.read_json(str(tmp_path))

    assert before.schema()["person"].dtype == expected_dtype
    assert after.schema()["person"].dtype == expected_dtype
    assert before.to_arrow() == after.to_arrow()


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native" or not PYARROW_GE_11_0_0,
    reason="JSON writes are only implemented in the native runner, and map types require PyArrow >= 11.0.0",
)
def test_roundtrip_map_types(tmp_path):
    map_data = [
        {"key1": "value1", "key2": "value2"},
        {"key1": "value3"},
        None,
    ]

    pa_map_type = pa.map_(pa.string(), pa.string())
    expected_dtype = DataType.map(DataType.string(), DataType.string())

    before = daft.from_arrow(pa.table({"id": pa.array(range(3)), "metadata": pa.array(map_data, type=pa_map_type)}))
    before = before.concat(before)
    before.write_json(str(tmp_path))
    after = daft.read_json(str(tmp_path))

    assert before.schema()["metadata"].dtype == expected_dtype
    # In JSON we cannot determine if a type is MAP and not STRUCT.
    # However, since casting from struct to map is not implemented, we'll just verify the data is preserved
    # by checking that the struct representation matches the expected structure.
    assert after.schema()["metadata"].dtype == DataType.struct({"key1": DataType.string(), "key2": DataType.string()})
    # Verify the data length is preserved.
    assert before.count_rows() == after.count_rows()


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_roundtrip_nested_struct_with_arrays(tmp_path):
    """Test JSON roundtrip with nested structs containing arrays."""
    nested_struct_data = [
        {"name": "Alice", "scores": [85, 90, 78], "tags": ["student", "active"]},
        {"name": "Bob", "scores": [92, 88, 95], "tags": ["student"]},
        None,
    ]

    pa_nested_struct_type = pa.struct(
        {"name": pa.string(), "scores": pa.list_(pa.int64()), "tags": pa.list_(pa.string())}
    )

    expected_dtype = DataType.struct(
        {"name": DataType.string(), "scores": DataType.list(DataType.int64()), "tags": DataType.list(DataType.string())}
    )

    before = daft.from_arrow(
        pa.table({"id": pa.array(range(3)), "student": pa.array(nested_struct_data, type=pa_nested_struct_type)})
    )
    before = before.concat(before)
    before.write_json(str(tmp_path))
    after = daft.read_json(str(tmp_path))
    assert before.schema()["student"].dtype == expected_dtype
    assert after.schema()["student"].dtype == expected_dtype
    assert before.to_arrow() == after.to_arrow()


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_throws_error_on_duration_and_binary_types(tmp_path):
    # TODO(desmond): Binary and Duration types currently produce inconsistent behaviours between our readers and writers.
    # Our readers expect BINARY to be encoded as plain text, and DURATION to be encoded with i64. Arrow-rs expects BINARY
    # to be hexadecimal encoded and DURATION to follow the ISO 8601 duration format. Until we reconcile this difference,
    # opt for throwing an error on encountering these types.
    duration_data = [
        datetime.timedelta(days=1),
        datetime.timedelta(days=2),
        None,
    ]

    pa_duration_type = pa.duration("ms")

    before_duration = daft.from_arrow(
        pa.table({"id": pa.array(range(3)), "duration": pa.array(duration_data, type=pa_duration_type)})
    )
    before_duration = before_duration.concat(before_duration)

    # Test that writing duration types throws NotImplementedError
    with pytest.raises(
        daft.exceptions.DaftCoreException,
        match="Not Yet Implemented: JSON writes are not supported with extension, timezone with timestamp, binary, or duration data types",
    ):
        before_duration.write_json(str(tmp_path))

    binary_data = [b"hello", b"world", None]
    pa_binary_type = pa.large_binary()
    before_binary = daft.from_arrow(
        pa.table({"id": pa.array(range(3)), "binary": pa.array(binary_data, type=pa_binary_type)})
    )

    with pytest.raises(
        daft.exceptions.DaftCoreException,
        match="Not Yet Implemented: JSON writes are not supported with extension, timezone with timestamp, binary, or duration data types",
    ):
        before_binary.write_json(str(tmp_path))
