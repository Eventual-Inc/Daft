from __future__ import annotations

import daft


def test_list_map():
    items = [["a", "b", "a"], ["b", "c", "b", "c"]]
    df = daft.from_pydict({"letters": items})
    expected = [["A", "B", "A"], ["B", "C", "B", "C"]]
    actual = df.select(daft.col("letters").list.map(daft.element().str.upper())).to_pydict()["letters"]
    assert actual == expected


def test_list_map_sql():
    items = [["a", "b", "a"], ["b", "c", "b", "c"]]
    df = daft.from_pydict({"letters": items})
    expected = [["A", "B", "A"], ["B", "C", "B", "C"]]
    actual = daft.sql("select list_map(letters, upper(element())) from df", df=df).to_pydict()["letters"]
    assert actual == expected


def test_list_map_with_udf():
    items = [["a", "b", "a"], ["b", "c", "b", "c"]]
    df = daft.from_pydict({"letters": items})
    expected = [["A", "B", "A"], ["B", "C", "B", "C"]]
    actual = df.select(
        daft.col("letters").list.map(daft.element().apply(lambda s: s.upper(), return_dtype=str))
    ).to_pydict()["letters"]
    assert actual == expected
