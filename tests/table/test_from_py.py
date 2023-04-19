from __future__ import annotations

import numpy as np
import pyarrow as pa
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
