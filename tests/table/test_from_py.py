from __future__ import annotations

import itertools

import numpy as np
import pyarrow as pa
import pyarrow.compute as pac
import pytest

from daft.series import Series
from daft.table import Table


def test_from_pydict_list() -> None:
    daft_table = Table.from_pydict({"a": [1, 2, 3]})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int64())


def test_from_pydict_np() -> None:
    daft_table = Table.from_pydict({"a": np.array([1, 2, 3], dtype=np.int64)})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int64())


def test_from_pydict_arrow() -> None:
    daft_table = Table.from_pydict({"a": pa.array([1, 2, 3], type=pa.int8())})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int8())


def test_from_pydict_arrow_list_array() -> None:
    arrow_arr = pa.array([[1, 2], [3], None, [None, 6]], pa.list_(pa.int64()))
    daft_table = Table.from_pydict({"a": arrow_arr})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pac.cast(arrow_arr, pa.large_list(pa.int64()))


def test_from_pydict_arrow_fixed_size_list_array() -> None:
    arrow_arr = pa.array([[1, 2], [3, 4], None, [None, 6]], pa.list_(pa.int64(), 2))
    daft_table = Table.from_pydict({"a": arrow_arr})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == arrow_arr


def test_from_pydict_arrow_struct_array() -> None:
    arrow_arr = pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}])
    daft_table = Table.from_pydict({"a": arrow_arr})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == arrow_arr


@pytest.mark.parametrize(
    "data,out_dtype",
    [
        (pa.array([1, 2, None, 4], type=pa.int64()), pa.int64()),
        (pa.array(["a", "b", None, "d"], type=pa.string()), pa.large_string()),
        (pa.array([b"a", b"b", None, b"d"], type=pa.binary()), pa.large_binary()),
        (pa.array([[1, 2], [3], None, [None, 4]], pa.list_(pa.int64())), pa.large_list(pa.int64())),
        (pa.array([[1, 2], [3, 4], None, [None, 6]], pa.list_(pa.int64(), 2)), pa.list_(pa.int64(), 2)),
        (
            pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}, None, {"a": 5, "c": 6}]),
            pa.struct([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())]),
        ),
    ],
)
@pytest.mark.parametrize("chunked", [False, True])
def test_from_pydict_arrow_with_nulls_roundtrip(data, out_dtype, chunked) -> None:
    if chunked:
        data = pa.chunked_array(data)
    daft_table = Table.from_pydict({"a": data})
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
        (pa.array([[1, 2], [3], [4, 5, 6], [None, 7]], pa.list_(pa.int64())), pa.large_list(pa.int64())),
        (pa.array([[1, 2], [3, None], [4, 5], [None, 6]], pa.list_(pa.int64(), 2)), pa.list_(pa.int64(), 2)),
        (
            pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}, {"a": 5}, {"a": 6, "c": 7}]),
            pa.struct([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())]),
        ),
        # Contains nulls.
        (pa.array([1, 2, None, 4], type=pa.int64()), pa.int64()),
        (pa.array(["a", "b", None, "d"], type=pa.string()), pa.large_string()),
        (pa.array([b"a", b"b", None, b"d"], type=pa.binary()), pa.large_binary()),
        (pa.array([[1, 2], [3], None, [None, 4]], pa.list_(pa.int64())), pa.large_list(pa.int64())),
        (pa.array([[1, 2], [3, 4], None, [None, 6]], pa.list_(pa.int64(), 2)), pa.list_(pa.int64(), 2)),
        (
            pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}, None, {"a": 5, "c": 6}]),
            pa.struct([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())]),
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
    daft_table = Table.from_pydict({"a": sliced_data})
    assert "a" in daft_table.column_names()
    if chunked:
        sliced_data = sliced_data.combine_chunks()
    assert daft_table.to_arrow()["a"].combine_chunks() == pac.cast(sliced_data, out_dtype)


def test_from_pydict_series() -> None:
    daft_table = Table.from_pydict({"a": Series.from_arrow(pa.array([1, 2, 3], type=pa.int8()))})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int8())


def test_from_arrow_round_trip() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]
    read_back = daft_table.to_arrow()
    assert pa_table == read_back


@pytest.mark.parametrize(
    "data,out_dtype",
    [
        # Full data.
        (pa.array([1, 2, 3, 4], type=pa.int64()), pa.int64()),
        (pa.array(["a", "b", "c", "d"], type=pa.string()), pa.large_string()),
        (pa.array([b"a", b"b", b"c", b"d"], type=pa.binary()), pa.large_binary()),
        (pa.array([[1, 2], [3], [4, 5, 6], [None, 7]], pa.list_(pa.int64())), pa.large_list(pa.int64())),
        (pa.array([[1, 2], [3, None], [4, 5], [None, 6]], pa.list_(pa.int64(), 2)), pa.list_(pa.int64(), 2)),
        (
            pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}, {"a": 5}, {"a": 6, "c": 7}]),
            pa.struct([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())]),
        ),
        # Contains nulls.
        (pa.array([1, 2, None, 4], type=pa.int64()), pa.int64()),
        (pa.array(["a", "b", None, "d"], type=pa.string()), pa.large_string()),
        (pa.array([b"a", b"b", None, b"d"], type=pa.binary()), pa.large_binary()),
        (pa.array([[1, 2], [3], None, [None, 4]], pa.list_(pa.int64())), pa.large_list(pa.int64())),
        (pa.array([[1, 2], [3, 4], None, [None, 6]], pa.list_(pa.int64(), 2)), pa.list_(pa.int64(), 2)),
        (
            pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}, None, {"a": 5, "c": 6}]),
            pa.struct([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())]),
        ),
    ],
)
@pytest.mark.parametrize("slice_", list(itertools.combinations(range(4), 2)))
def test_from_arrow_sliced_roundtrip(data, out_dtype, slice_) -> None:
    offset, end = slice_
    length = end - offset
    sliced_data = data.slice(offset, length)
    daft_table = Table.from_arrow(pa.table({"a": sliced_data}))
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pac.cast(sliced_data, out_dtype)


def test_from_pydict_bad_input() -> None:
    with pytest.raises(ValueError, match="Mismatch in Series lengths"):
        Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7]})


def test_pyobjects_roundtrip() -> None:
    o0, o1 = object(), object()
    table = Table.from_pydict({"objs": [o0, o1, None]})
    objs = table.to_pydict()["objs"]
    assert objs[0] is o0
    assert objs[1] is o1
    assert objs[2] is None
