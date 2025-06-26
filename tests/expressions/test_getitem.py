from __future__ import annotations

import daft
from daft import col


def test_getitem_list():
    df = daft.from_pydict({"a": [[1, 2, 3], [1], [1, 2]]})
    df = df.select(df["a"][0].alias("first"), col("a")[1].alias("second"))
    assert df.to_pydict() == {"first": [1, 1, 1], "second": [2, None, 2]}


def test_getitem_struct():
    df = daft.from_pydict({"a": [{"x": 1, "y": "foo"}, {"x": 2, "y": "bar"}]})
    df = df.select(col("a")["x"], df["a"]["y"])
    assert df.to_pydict() == {"x": [1, 2], "y": ["foo", "bar"]}


def test_getitem_struct_all():
    df = daft.from_pydict({"a": [{"x": 1, "y": "foo"}, {"x": 2, "y": "bar"}]})
    df = df.select(df["a"]["*"])
    assert df.to_pydict() == {"x": [1, 2], "y": ["foo", "bar"]}
