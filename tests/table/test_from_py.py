from __future__ import annotations

import datetime
import itertools

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pac
import pytest

from daft import DataType, TimeUnit
from daft.context import get_context
from daft.series import Series
from daft.table import MicroPartition
from daft.utils import pyarrow_supports_fixed_shape_tensor

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())

PYTHON_TYPE_ARRAYS = {
    "int": [1, 2],
    "float": [1.0, 2.0],
    "bool": [True, False],
    "str": ["foo", "bar"],
    "binary": [b"foo", b"bar"],
    "date": [datetime.date.today(), datetime.date.today()],
    "time": [datetime.time(1, 2, 3, 4), datetime.time(5, 6, 7, 8)],
    "list": [[1, 2], [3]],
    "struct": [{"a": 1, "b": 2.0}, {"b": 3.0}],
    "empty_struct": [{}, {}],
    "nested_struct": [{"a": {"b": 1}, "c": {}}, {"a": {"b": 3}, "c": {}}],
    "null": [None, None],
    "tensor": [np.ones((2, 2), np.int64), np.ones((3, 3), np.int64)],
    # The following types are not natively supported and will be cast to Python object types.
    "timestamp": [datetime.datetime.now(), datetime.datetime.now()],
}

PYTHON_INFERRED_TYPES = {
    "int": DataType.int64(),
    "float": DataType.float64(),
    "bool": DataType.bool(),
    "str": DataType.string(),
    "binary": DataType.binary(),
    "date": DataType.date(),
    "time": DataType.time(TimeUnit.us()),
    "list": DataType.list(DataType.int64()),
    "struct": DataType.struct({"a": DataType.int64(), "b": DataType.float64()}),
    "empty_struct": DataType.struct({"": DataType.null()}),
    "nested_struct": DataType.struct(
        {"a": DataType.struct({"b": DataType.int64()}), "c": DataType.struct({"": DataType.null()})}
    ),
    "null": DataType.null(),
    "tensor": DataType.tensor(DataType.int64()),
    # The following types are not natively supported and will be cast to Python object types.
    "timestamp": DataType.timestamp(TimeUnit.us()),
}

PANDAS_INFERRED_TYPES = {
    **PYTHON_INFERRED_TYPES,
    "timestamp": DataType.timestamp(TimeUnit.ns()),
}

ROUNDTRIP_TYPES = {
    "int": pa.int64(),
    "float": pa.float64(),
    "bool": pa.bool_(),
    "str": pa.large_string(),
    "binary": pa.large_binary(),
    "date": pa.date32(),
    "time": pa.time64("us"),
    "list": pa.large_list(pa.int64()),
    "struct": pa.struct({"a": pa.int64(), "b": pa.float64()}),
    "empty_struct": pa.struct({}),
    "nested_struct": pa.struct({"a": pa.struct({"b": pa.int64()}), "c": pa.struct({})}),
    "null": pa.null(),
    "tensor": PYTHON_INFERRED_TYPES["tensor"].to_arrow_dtype(),
    # The following types are not natively supported and will be cast to Python object types.
    "timestamp": pa.timestamp("us"),
}


ARROW_TYPE_ARRAYS = {
    "int8": pa.array(PYTHON_TYPE_ARRAYS["int"], pa.int8()),
    "int16": pa.array(PYTHON_TYPE_ARRAYS["int"], pa.int16()),
    "int32": pa.array(PYTHON_TYPE_ARRAYS["int"], pa.int32()),
    "int64": pa.array(PYTHON_TYPE_ARRAYS["int"], pa.int64()),
    "uint8": pa.array(PYTHON_TYPE_ARRAYS["int"], pa.uint8()),
    "uint16": pa.array(PYTHON_TYPE_ARRAYS["int"], pa.uint16()),
    "uint32": pa.array(PYTHON_TYPE_ARRAYS["int"], pa.uint32()),
    "uint64": pa.array(PYTHON_TYPE_ARRAYS["int"], pa.uint64()),
    "float32": pa.array(PYTHON_TYPE_ARRAYS["float"], pa.float32()),
    "float64": pa.array(PYTHON_TYPE_ARRAYS["float"], pa.float64()),
    "string": pa.array(PYTHON_TYPE_ARRAYS["str"], pa.string()),
    "binary": pa.array(PYTHON_TYPE_ARRAYS["binary"], pa.binary()),
    "fixed_size_binary": pa.array(PYTHON_TYPE_ARRAYS["binary"], pa.binary(3)),
    "boolean": pa.array(PYTHON_TYPE_ARRAYS["bool"], pa.bool_()),
    "date32": pa.array(PYTHON_TYPE_ARRAYS["date"], pa.date32()),
    "date64": pa.array(PYTHON_TYPE_ARRAYS["date"], pa.date64()),
    "time64_microseconds": pa.array(PYTHON_TYPE_ARRAYS["time"], pa.time64("us")),
    "time64_nanoseconds": pa.array(PYTHON_TYPE_ARRAYS["time"], pa.time64("ns")),
    "list": pa.array(PYTHON_TYPE_ARRAYS["list"], pa.list_(pa.int64())),
    "fixed_size_list": pa.array([[1, 2], [3, 4]], pa.list_(pa.int64(), 2)),
    "map": pa.array(
        [[(1.0, 1), (2.0, 2)], [(3.0, 3), (4.0, 4)]],
        pa.map_(pa.float32(), pa.int32()),
    ),
    "struct": pa.array(PYTHON_TYPE_ARRAYS["struct"], pa.struct([("a", pa.int64()), ("b", pa.float64())])),
    "empty_struct": pa.array(PYTHON_TYPE_ARRAYS["empty_struct"], pa.struct([])),
    "nested_struct": pa.array(
        PYTHON_TYPE_ARRAYS["nested_struct"],
        pa.struct(
            {
                "a": pa.struct([("b", pa.int64())]),
                "c": pa.struct([]),
            }
        ),
    ),
    "null": pa.array(PYTHON_TYPE_ARRAYS["null"], pa.null()),
    "tensor": pa.ExtensionArray.from_storage(
        ROUNDTRIP_TYPES["tensor"],
        pa.array(
            [
                {"data": PYTHON_TYPE_ARRAYS["tensor"][0].ravel(), "shape": [2, 2]},
                {"data": PYTHON_TYPE_ARRAYS["tensor"][1].ravel(), "shape": [3, 3]},
            ],
            pa.struct(
                {
                    "data": pa.large_list(pa.field("item", pa.int64())),
                    "shape": pa.large_list(pa.field("item", pa.uint64())),
                }
            ),
        ),
    ),
    # The following types are not natively supported and will be cast to Python object types.
    "timestamp": pa.array(PYTHON_TYPE_ARRAYS["timestamp"]),
}


ARROW_ROUNDTRIP_TYPES = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.large_string(),
    "binary": pa.large_binary(),
    "fixed_size_binary": pa.binary(3),
    "boolean": pa.bool_(),
    "date32": pa.date32(),
    "date64": pa.timestamp("ms"),
    "time64_microseconds": pa.time64("us"),
    "time64_nanoseconds": pa.time64("ns"),
    "list": pa.large_list(pa.int64()),
    "fixed_size_list": pa.list_(pa.int64(), 2),
    "map": pa.map_(pa.float32(), pa.int32()),
    "struct": pa.struct([("a", pa.int64()), ("b", pa.float64())]),
    "empty_struct": pa.struct([]),
    "nested_struct": pa.struct([("a", pa.struct([("b", pa.int64())])), ("c", pa.struct([]))]),
    "null": pa.null(),
    "tensor": PYTHON_INFERRED_TYPES["tensor"].to_arrow_dtype(),
    # The following types are not natively supported and will be cast to Python object types.
    "timestamp": pa.timestamp("us"),
}

if pyarrow_supports_fixed_shape_tensor():
    arrow_tensor_dtype = pa.fixed_shape_tensor(pa.int64(), (2, 2))
    # NOTE: We don't infer fixed-shape tensors when constructing a table from Python objects, since
    # the shapes may be variable across partitions.
    # PYTHON_TYPE_ARRAYS["canonical_tensor"] = list(np.arange(8).reshape(2, 2, 2))
    # PYTHON_INFERRED_TYPES["canonical_tensor"] = DataType.tensor(DataType.int64(), (2, 2))
    # ROUNDTRIP_TYPES["canonical_tensor"] = arrow_tensor_dtype
    ARROW_ROUNDTRIP_TYPES["canonical_tensor"] = arrow_tensor_dtype
    ARROW_TYPE_ARRAYS["canonical_tensor"] = pa.FixedShapeTensorArray.from_numpy_ndarray(np.arange(8).reshape(2, 2, 2))


def _with_uuid_ext_type(uuid_ext_type) -> tuple[dict, dict]:
    if get_context().runner_config.name == "ray":
        # pyarrow extension types aren't supported in Ray clusters yet.
        return ARROW_ROUNDTRIP_TYPES, ARROW_TYPE_ARRAYS
    arrow_roundtrip_types = ARROW_ROUNDTRIP_TYPES.copy()
    arrow_type_arrays = ARROW_TYPE_ARRAYS.copy()
    arrow_roundtrip_types["ext_type"] = uuid_ext_type
    storage = ARROW_TYPE_ARRAYS["binary"]
    arrow_type_arrays["ext_type"] = pa.ExtensionArray.from_storage(uuid_ext_type, storage)
    return arrow_roundtrip_types, arrow_type_arrays


def test_from_pydict_roundtrip() -> None:
    table = MicroPartition.from_pydict(PYTHON_TYPE_ARRAYS)
    assert len(table) == 2
    assert set(table.column_names()) == set(PYTHON_TYPE_ARRAYS.keys())
    for field in table.schema():
        assert field.dtype == PYTHON_INFERRED_TYPES[field.name]
    schema = pa.schema(ROUNDTRIP_TYPES)
    arrs = {}
    for col_name, col in PYTHON_TYPE_ARRAYS.items():
        if col_name == "tensor":
            arrs[col_name] = ARROW_TYPE_ARRAYS[col_name]
        else:
            arrs[col_name] = pa.array(col, type=schema.field(col_name).type)
    expected_table = pa.table(arrs, schema=schema)
    assert table.to_arrow() == expected_table


def test_from_pydict_arrow_roundtrip(uuid_ext_type) -> None:
    arrow_roundtrip_types, arrow_type_arrays = _with_uuid_ext_type(uuid_ext_type)
    table = MicroPartition.from_pydict(arrow_type_arrays)
    assert len(table) == 2
    assert set(table.column_names()) == set(arrow_type_arrays.keys())
    for field in table.schema():
        assert field.dtype == (
            DataType.from_arrow_type(arrow_type_arrays[field.name].type)
            if (field.name != "empty_struct" and field.name != "nested_struct")
            else PYTHON_INFERRED_TYPES[field.name]  # empty structs are internally represented as {"": None}
        )
    expected_table = pa.table(arrow_type_arrays).cast(pa.schema(arrow_roundtrip_types))
    assert table.to_arrow() == expected_table


def test_from_arrow_roundtrip(uuid_ext_type) -> None:
    arrow_roundtrip_types, arrow_type_arrays = _with_uuid_ext_type(uuid_ext_type)
    pa_table = pa.table(arrow_type_arrays)
    table = MicroPartition.from_arrow(pa_table)
    assert len(table) == 2
    assert set(table.column_names()) == set(arrow_type_arrays.keys())
    for field in table.schema():
        assert field.dtype == (
            DataType.from_arrow_type(arrow_type_arrays[field.name].type)
            if (field.name != "empty_struct" and field.name != "nested_struct")
            else PYTHON_INFERRED_TYPES[field.name]  # empty structs are internally represented as {"": None}
        )
    expected_table = pa.table(arrow_type_arrays).cast(pa.schema(arrow_roundtrip_types))
    assert table.to_arrow() == expected_table


def test_from_pandas_roundtrip() -> None:
    df = pd.DataFrame(PYTHON_TYPE_ARRAYS)
    table = MicroPartition.from_pandas(df)
    assert len(table) == 2
    assert set(table.column_names()) == set(PYTHON_TYPE_ARRAYS.keys())
    for field in table.schema():
        assert field.dtype == PANDAS_INFERRED_TYPES[field.name]
    # pyarrow --> pandas will insert explicit Nones within the struct fields.
    df["struct"][1]["a"] = None
    df["empty_struct"][0] = {}
    df["empty_struct"][1] = {}
    df["nested_struct"][0]["c"] = {}
    df["nested_struct"][1]["c"] = {}
    pd.testing.assert_frame_equal(table.to_pandas(), df)


def test_from_pydict_list() -> None:
    daft_table = MicroPartition.from_pydict({"a": [1, 2, 3]})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int64())


def test_from_pydict_np() -> None:
    daft_table = MicroPartition.from_pydict({"a": np.array([1, 2, 3], dtype=np.int64)})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int64())


def test_from_pydict_arrow() -> None:
    daft_table = MicroPartition.from_pydict({"a": pa.array([1, 2, 3], type=pa.int8())})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int8())


@pytest.mark.parametrize("list_type", [pa.list_, pa.large_list])
def test_from_pydict_arrow_list_array(list_type) -> None:
    arrow_arr = pa.array([["a", "b"], ["c"], None, [None, "d", "e"]], list_type(pa.string()))
    daft_table = MicroPartition.from_pydict({"a": arrow_arr})
    assert "a" in daft_table.column_names()
    # Perform expected Daft cast, where the outer list array is cast to a large list array
    # (if the outer list array wasn't already a large list in the first place), and
    # the inner string array is cast to a large string array.
    expected = arrow_arr.cast(pa.large_list(pa.large_string()))
    assert daft_table.to_arrow()["a"].combine_chunks() == expected


def test_from_pydict_arrow_fixed_size_list_array() -> None:
    data = [["a", "b"], ["c", "d"], None, [None, "e"]]
    arrow_arr = pa.array(data, pa.list_(pa.string(), 2))
    daft_table = MicroPartition.from_pydict({"a": arrow_arr})
    assert "a" in daft_table.column_names()
    # Perform expected Daft cast, where the inner string array is cast to a large string array.
    expected = pa.array(data, type=pa.list_(pa.large_string(), 2))
    assert daft_table.to_arrow()["a"].combine_chunks() == expected


def test_from_pydict_arrow_map_array() -> None:
    data = [[(1, 2.0), (3, 4.0)], None, [(5, 6.0), (7, 8.0)]]
    arrow_arr = pa.array(data, pa.map_(pa.int64(), pa.float64()))
    daft_table = MicroPartition.from_pydict({"a": arrow_arr})
    assert "a" in daft_table.column_names()
    # Perform expected Daft cast, where the inner string and int arrays are cast to large string and int arrays.
    expected = arrow_arr.cast(pa.map_(pa.int64(), pa.float64()))
    assert daft_table.to_arrow()["a"].combine_chunks() == expected
    assert daft_table.to_pydict()["a"] == data


def test_from_pydict_arrow_struct_array() -> None:
    data = [{"a": "foo", "b": "bar"}, {"b": "baz", "c": "quux"}]
    arrow_arr = pa.array(data)
    daft_table = MicroPartition.from_pydict({"a": arrow_arr})
    assert "a" in daft_table.column_names()
    # Perform expected Daft cast, where the inner string array is cast to a large string array.
    expected = pa.array(
        data, type=pa.struct([("a", pa.large_string()), ("b", pa.large_string()), ("c", pa.large_string())])
    )
    assert daft_table.to_arrow()["a"].combine_chunks() == expected


@pytest.mark.skipif(
    get_context().runner_config.name == "ray",
    reason="pyarrow extension types aren't supported on Ray clusters.",
)
def test_from_pydict_arrow_extension_array(uuid_ext_type) -> None:
    pydata = [f"{i}".encode() for i in range(6)]
    pydata[2] = None
    storage = pa.array(pydata)
    arrow_arr = pa.ExtensionArray.from_storage(uuid_ext_type, storage)
    daft_table = MicroPartition.from_pydict({"a": arrow_arr})
    assert "a" in daft_table.column_names()
    # Although Daft will internally represent the binary storage array as a large_binary array,
    # it should be cast back to the ingress extension type.
    result = daft_table.to_arrow()["a"]
    assert result.type == uuid_ext_type
    assert result.to_pylist() == arrow_arr.to_pylist()


def test_from_pydict_arrow_deeply_nested() -> None:
    # Test a struct of lists of struct of lists of strings.
    data = [{"a": [{"b": ["foo", "bar"]}]}, {"a": [{"b": ["baz", "quux"]}]}]
    arrow_arr = pa.array(data)
    daft_table = MicroPartition.from_pydict({"a": arrow_arr})
    assert "a" in daft_table.column_names()
    # Perform the expected Daft cast, where each list array is cast to a large list array and
    # the string array is cast to a large string array.
    expected = pa.array(
        data,
        type=pa.struct([("a", pa.large_list(pa.field("item", pa.struct([("b", pa.large_list(pa.large_string()))]))))]),
    )
    assert daft_table.to_arrow()["a"].combine_chunks() == expected


@pytest.mark.parametrize(
    "data,out_dtype",
    [
        (pa.array([1, 2, None, 4], type=pa.int64()), pa.int64()),
        (pa.array(["a", "b", None, "d"], type=pa.string()), pa.large_string()),
        (pa.array([b"a", b"b", None, b"d"], type=pa.binary()), pa.large_binary()),
        (pa.array([b"a", b"b", None, b"d"], type=pa.binary(1)), pa.binary(1)),
        (pa.array([[1, 2], [3], None, [None, 4]], pa.list_(pa.int64())), pa.large_list(pa.int64())),
        (pa.array([[1, 2], [3, 4], None, [None, 6]], pa.list_(pa.int64(), 2)), pa.list_(pa.int64(), 2)),
        (
            pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}, None, {"a": 5, "c": 6}]),
            pa.struct([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())]),
        ),
        (
            pa.array([[(1, 2), (3, 4)], None, [(5, 6), (7, 8)]], pa.map_(pa.int64(), pa.int64())),
            pa.map_(pa.int64(), pa.int64()),
        ),
    ],
)
@pytest.mark.parametrize("chunked", [False, True])
def test_from_pydict_arrow_with_nulls_roundtrip(data, out_dtype, chunked) -> None:
    if chunked:
        data = pa.chunked_array(data)
    daft_table = MicroPartition.from_pydict({"a": data})
    assert "a" in daft_table.column_names()
    if chunked:
        data = data.combine_chunks()
    assert daft_table.to_arrow()["a"].combine_chunks() == pac.cast(data, out_dtype)


@pytest.mark.parametrize(
    "data,out_dtype",
    [
        # Full data.
        (pa.array([1, 2, 3, 4], type=pa.int64()), pa.int64()),
        (pa.array(["a", "b", "c", "d"], type=pa.string()), pa.large_string()),
        (pa.array([b"a", b"b", b"c", b"d"], type=pa.binary()), pa.large_binary()),
        (pa.array([b"a", b"b", b"c", b"d"], type=pa.binary(1)), pa.binary(1)),
        (pa.array([[1, 2], [3], [4, 5, 6], [None, 7]], pa.list_(pa.int64())), pa.large_list(pa.int64())),
        (pa.array([[1, 2], [3, None], [4, 5], [None, 6]], pa.list_(pa.int64(), 2)), pa.list_(pa.int64(), 2)),
        (
            pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}, {"a": 5}, {"a": 6, "c": 7}]),
            pa.struct([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())]),
        ),
        (
            pa.array([[(1, 2), (3, 4)], [(5, 6)], [(7, 8)]], pa.map_(pa.int64(), pa.int64())),
            pa.map_(pa.int64(), pa.int64()),
        ),
        # Contains nulls.
        (pa.array([1, 2, None, 4], type=pa.int64()), pa.int64()),
        (pa.array(["a", "b", None, "d"], type=pa.string()), pa.large_string()),
        (pa.array([b"a", b"b", None, b"d"], type=pa.binary()), pa.large_binary()),
        (pa.array([b"a", b"b", None, b"d"], type=pa.binary(1)), pa.binary(1)),
        (pa.array([[1, 2], [3], None, [None, 4]], pa.list_(pa.int64())), pa.large_list(pa.int64())),
        (pa.array([[1, 2], [3, 4], None, [None, 6]], pa.list_(pa.int64(), 2)), pa.list_(pa.int64(), 2)),
        (
            pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}, None, {"a": 5, "c": 6}]),
            pa.struct([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())]),
        ),
        (
            pa.array([[(1, 2), (3, 4)], None, [(7, 8)]], pa.map_(pa.int64(), pa.int64())),
            pa.map_(pa.int64(), pa.int64()),
        ),
    ],
)
@pytest.mark.parametrize("chunked", [False, True])
@pytest.mark.parametrize("slice_", list(itertools.combinations(range(4), 2)))
def test_from_pydict_arrow_sliced_roundtrip(data, out_dtype, chunked, slice_) -> None:
    offset, end = slice_
    length = end - offset
    sliced_data = data.slice(offset, length)
    if chunked:
        sliced_data = pa.chunked_array(sliced_data)
    daft_table = MicroPartition.from_pydict({"a": sliced_data})
    assert "a" in daft_table.column_names()
    if chunked:
        sliced_data = sliced_data.combine_chunks()
    assert daft_table.to_arrow()["a"].combine_chunks() == pac.cast(sliced_data, out_dtype)


def test_from_pydict_series() -> None:
    daft_table = MicroPartition.from_pydict({"a": Series.from_arrow(pa.array([1, 2, 3], type=pa.int8()))})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int8())


@pytest.mark.parametrize(
    "data,out_dtype",
    [
        # Full data.
        (pa.array([1, 2, 3, 4], type=pa.int64()), pa.int64()),
        (pa.array(["a", "b", "c", "d"], type=pa.string()), pa.large_string()),
        (pa.array([b"a", b"b", b"c", b"d"], type=pa.binary()), pa.large_binary()),
        (pa.array([b"a", b"b", b"c", b"d"], type=pa.binary(1)), pa.binary(1)),
        (pa.array([[1, 2], [3], [4, 5, 6], [None, 7]], pa.list_(pa.int64())), pa.large_list(pa.int64())),
        (pa.array([[1, 2], [3, None], [4, 5], [None, 6]], pa.list_(pa.int64(), 2)), pa.list_(pa.int64(), 2)),
        (
            pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}, {"a": 5}, {"a": 6, "c": 7}]),
            pa.struct([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())]),
        ),
        (
            pa.array([[(1, 2), (3, 4)], [(5, 6)], [(7, 8)]], pa.map_(pa.int64(), pa.int64())),
            pa.map_(pa.int64(), pa.int64()),
        ),
        # Contains nulls.
        (pa.array([1, 2, None, 4], type=pa.int64()), pa.int64()),
        (pa.array(["a", "b", None, "d"], type=pa.string()), pa.large_string()),
        (pa.array([b"a", b"b", None, b"d"], type=pa.binary()), pa.large_binary()),
        (pa.array([b"a", b"b", None, b"d"], type=pa.binary(1)), pa.binary(1)),
        (pa.array([[1, 2], [3], None, [None, 4]], pa.list_(pa.int64())), pa.large_list(pa.int64())),
        (pa.array([[1, 2], [3, 4], None, [None, 6]], pa.list_(pa.int64(), 2)), pa.list_(pa.int64(), 2)),
        (
            pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}, None, {"a": 5, "c": 6}]),
            pa.struct([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())]),
        ),
        (
            pa.array([[(1, 2), (3, 4)], None, [(7, 8)]], pa.map_(pa.int64(), pa.int64())),
            pa.map_(pa.int64(), pa.int64()),
        ),
    ],
)
@pytest.mark.parametrize("slice_", list(itertools.combinations(range(4), 2)))
def test_from_arrow_sliced_roundtrip(data, out_dtype, slice_) -> None:
    offset, end = slice_
    length = end - offset
    sliced_data = data.slice(offset, length)
    daft_table = MicroPartition.from_arrow(pa.table({"a": sliced_data}))
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pac.cast(sliced_data, out_dtype)


@pytest.mark.parametrize("list_type", [pa.list_, pa.large_list])
def test_from_arrow_list_array(list_type) -> None:
    arrow_arr = pa.array([["a", "b"], ["c"], None, [None, "d", "e"]], list_type(pa.string()))
    daft_table = MicroPartition.from_arrow(pa.table({"a": arrow_arr}))
    assert "a" in daft_table.column_names()
    # Perform expected Daft cast, where the outer list array is cast to a large list array
    # (if the outer list array wasn't already a large list in the first place), and
    # the inner string array is cast to a large string array.
    expected = arrow_arr.cast(pa.large_list(pa.large_string()))
    assert daft_table.to_arrow()["a"].combine_chunks() == expected


def test_from_arrow_fixed_size_list_array() -> None:
    data = [["a", "b"], ["c", "d"], None, [None, "e"]]
    arrow_arr = pa.array(data, pa.list_(pa.string(), 2))
    daft_table = MicroPartition.from_arrow(pa.table({"a": arrow_arr}))
    assert "a" in daft_table.column_names()
    # Perform expected Daft cast, where the inner string array is cast to a large string array.
    expected = pa.array(data, type=pa.list_(pa.large_string(), 2))
    assert daft_table.to_arrow()["a"].combine_chunks() == expected


def test_from_arrow_struct_array() -> None:
    data = [{"a": "foo", "b": "bar"}, {"b": "baz", "c": "quux"}]
    arrow_arr = pa.array(data)
    daft_table = MicroPartition.from_arrow(pa.table({"a": arrow_arr}))
    assert "a" in daft_table.column_names()
    # Perform expected Daft cast, where the inner string array is cast to a large string array.
    expected = pa.array(
        data, type=pa.struct([("a", pa.large_string()), ("b", pa.large_string()), ("c", pa.large_string())])
    )
    assert daft_table.to_arrow()["a"].combine_chunks() == expected


def test_from_arrow_map_array() -> None:
    data = [[(1.0, 1), (2.0, 2)], [(3.0, 3), (4.0, 4)]]
    arrow_arr = pa.array(data, pa.map_(pa.float32(), pa.int32()))
    daft_table = MicroPartition.from_arrow(pa.table({"a": arrow_arr}))
    assert "a" in daft_table.column_names()
    # Perform expected Daft cast, where the inner string and int arrays are cast to large string and int arrays.
    expected = arrow_arr.cast(pa.map_(pa.float32(), pa.int32()))
    assert daft_table.to_arrow()["a"].combine_chunks() == expected
    assert daft_table.to_pydict()["a"] == data


@pytest.mark.skipif(
    get_context().runner_config.name == "ray",
    reason="pyarrow extension types aren't supported on Ray clusters.",
)
def test_from_arrow_extension_array(uuid_ext_type) -> None:
    pydata = [f"{i}".encode() for i in range(6)]
    pydata[2] = None
    storage = pa.array(pydata)
    arrow_arr = pa.ExtensionArray.from_storage(uuid_ext_type, storage)
    daft_table = MicroPartition.from_arrow(pa.table({"a": arrow_arr}))
    assert "a" in daft_table.column_names()
    # Although Daft will internally represent the binary storage array as a large_binary array,
    # it should be cast back to the ingress extension type.
    result = daft_table.to_arrow()["a"]
    assert result.type == uuid_ext_type
    assert result.to_pylist() == arrow_arr.to_pylist()


def test_from_arrow_deeply_nested() -> None:
    # Test a struct of lists of struct of lists of strings.
    data = [{"a": [{"b": ["foo", "bar"]}]}, {"a": [{"b": ["baz", "quux"]}]}]
    arrow_arr = pa.array(data)
    daft_table = MicroPartition.from_arrow(pa.table({"a": arrow_arr}))
    assert "a" in daft_table.column_names()
    # Perform the expected Daft cast, where each list array is cast to a large list array and
    # the string array is cast to a large string array.
    expected = pa.array(
        data,
        type=pa.struct(
            [
                (
                    "a",
                    pa.large_list(
                        pa.field("item", pa.struct([("b", pa.large_list(pa.field("item", pa.large_string())))]))
                    ),
                )
            ]
        ),
    )

    assert daft_table.to_arrow()["a"].combine_chunks() == expected


def test_from_pydict_bad_input() -> None:
    with pytest.raises(ValueError, match="Mismatch in Series lengths"):
        MicroPartition.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7]})


def test_pyobjects_roundtrip() -> None:
    o0, o1 = object(), object()
    table = MicroPartition.from_pydict({"objs": [o0, o1, None]})
    objs = table.to_pydict()["objs"]
    assert objs[0] is o0
    assert objs[1] is o1
    assert objs[2] is None


@pytest.mark.parametrize("levels", list(range(6)))
def test_nested_list_dates(levels: int) -> None:
    data = [datetime.date.today(), datetime.date.today()]
    for _ in range(levels):
        data = [data, data]
    table = MicroPartition.from_pydict({"item": data})
    back_again = table.get_column("item")

    dtype = back_again.datatype()

    expected_dtype = DataType.date()
    expected_arrow_type = pa.date32()
    for _ in range(levels):
        expected_dtype = DataType.list(expected_dtype)
        expected_arrow_type = pa.large_list(pa.field("item", expected_arrow_type))

    assert dtype == expected_dtype

    assert back_again.to_arrow().type == expected_arrow_type
    assert back_again.to_pylist() == data


# TODO: Fix downcasting for FixedSizedList in Series::try_from
@pytest.mark.skip()
@pytest.mark.parametrize("levels", list(range(1, 6)))
def test_nested_fixed_size_list_dates(levels: int) -> None:
    data = [datetime.date.today(), datetime.date.today()]
    for _ in range(levels):
        data = [data, data]

    expected_dtype = DataType.date()
    expected_arrow_type = pa.date32()
    for _ in range(levels):
        expected_dtype = DataType.fixed_size_list(expected_dtype, 2)
        expected_arrow_type = pa.list_(expected_arrow_type, 2)

    pa_data = pa.array(data, type=expected_arrow_type)
    table = MicroPartition.from_pydict({"data": pa_data})
    back_again = table.get_column("data")

    dtype = back_again.datatype()

    assert dtype == expected_dtype
    assert back_again.to_arrow().type == expected_arrow_type
    assert back_again.to_pylist() == data


@pytest.mark.parametrize("levels", list(range(5)))
def test_nested_struct_dates(levels: int) -> None:
    data = datetime.date.today()
    for _ in range(levels):
        data = {"data": data}

    data = [data]
    table = MicroPartition.from_pydict({"data": data})
    back_again = table.get_column("data")
    dtype = back_again.datatype()
    expected_dtype = DataType.date()
    expected_arrow_type = pa.date32()
    for _ in range(levels):
        expected_dtype = DataType.struct({"data": expected_dtype})
        expected_arrow_type = pa.struct([("data", expected_arrow_type)])
    assert dtype == expected_dtype

    assert back_again.to_arrow().type == expected_arrow_type
    assert back_again.to_pylist() == data
