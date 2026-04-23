from __future__ import annotations

import daft


def test_list_filter():
    df = daft.from_pydict({"letters": [["a", "b", "a"], ["b", "c", "b", "c"]]})
    actual = df.select(daft.col("letters").list_filter(daft.element() != "b")).to_pydict()["letters"]
    assert actual == [["a", "a"], ["c", "c"]]


def test_list_filter_sql():
    df = daft.from_pydict({"letters": [["a", "b", "a"], ["b", "c", "b", "c"]]})
    actual = daft.sql("select list_filter(letters, element() != 'b') as letters from df", df=df).to_pydict()["letters"]
    assert actual == [["a", "a"], ["c", "c"]]


def test_list_filter_numeric():
    df = daft.from_pydict({"nums": [[1, 2, 3, 4], [5, 6, 7]]})
    actual = df.select(daft.col("nums").list_filter(daft.element() % 2 == 0)).to_pydict()["nums"]
    assert actual == [[2, 4], [6]]


def test_list_filter_empty_rows():
    df = daft.from_pydict({"xs": [[], [1, 2], [3]]})
    actual = df.select(daft.col("xs").list_filter(daft.element() > 1)).to_pydict()["xs"]
    assert actual == [[], [2], [3]]


def test_list_filter_all_excluded():
    df = daft.from_pydict({"xs": [[1, 2, 3]]})
    actual = df.select(daft.col("xs").list_filter(daft.element() > 100)).to_pydict()["xs"]
    assert actual == [[]]


def test_list_filter_null_list_row():
    df = daft.from_pydict({"xs": [[1, 2, 3], None, [4]]})
    actual = df.select(daft.col("xs").list_filter(daft.element() != 2)).to_pydict()["xs"]
    assert actual == [[1, 3], None, [4]]


def test_list_filter_chain_with_map():
    df = daft.from_pydict({"text": [["hello", "world"], ["goodbye", "world"]]})
    actual = df.select(
        daft.col("text").list_filter(daft.element() != "world").list_map(daft.element().upper())
    ).to_pydict()["text"]
    assert actual == [["HELLO"], ["GOODBYE"]]


def test_list_map_then_filter():
    df = daft.from_pydict({"nums": [[1, 2, 3], [4, 5]]})
    actual = df.select(daft.col("nums").list_map(daft.element() * 2).list_filter(daft.element() > 4)).to_pydict()[
        "nums"
    ]
    assert actual == [[6], [8, 10]]


def test_list_filter_struct_field():
    df = daft.from_pydict(
        {
            "structs": [
                [{"a": 1, "b": "x"}, {"a": 5, "b": "y"}],
                [{"a": 0, "b": "z"}],
                [],
            ]
        }
    )
    res = df.select(daft.col("structs").list_filter(daft.element().get("a") > 0)).to_pydict()["structs"]
    assert res == [[{"a": 1, "b": "x"}, {"a": 5, "b": "y"}], [], []]
