from __future__ import annotations

import pytest

import daft


def test_simple_concat():
    df1 = daft.from_pydict({"foo": [1, 2, 3]})
    df2 = daft.from_pydict({"foo": [4, 5, 6]})
    result = df1.concat(df2)
    assert result.to_pydict() == {"foo": [1, 2, 3, 4, 5, 6]}


def test_concat_schema_mismatch():
    df1 = daft.from_pydict({"foo": [1, 2, 3]})
    df2 = daft.from_pydict({"foo": ["4", "5", "6"]})
    with pytest.raises(ValueError):
        df1.concat(df2)
