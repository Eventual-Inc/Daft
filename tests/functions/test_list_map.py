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


def test_map_chaining():
    df = daft.from_pydict({"numbers": [[1, 2, 3], [4, 5, 6, 7]]})
    actual = df.select(daft.col("numbers").list.map(daft.element() * 2).list.map(daft.element() * 4)).to_pydict()[
        "numbers"
    ]

    expected = [[8, 16, 24], [32, 40, 48, 56]]

    assert actual == expected


def test_map_nested():
    df = daft.from_pydict({"sentences": [["this is a test", "another test"]]})

    words = (
        df.select(daft.col("sentences").list.map(daft.element().str.split(" ")).alias("words"))
        .explode("words")
        .explode("words")
        .to_pydict()["words"]
    )

    expected = ["this", "is", "a", "test", "another", "test"]

    assert words == expected


def test_list_map_struct_field_extraction_without_alias():
    df = daft.from_pydict(
        {
            "structs": [
                [{"a": "a1", "b": "b1"}, {"a": "a2", "b": "b2"}],
                [{"a": "a3", "b": "b3"}],
                [],
            ]
        }
    )

    res = df.select(daft.col("structs").list.map(daft.element().struct.get("a"))).to_pydict()["a"]

    expected = [["a1", "a2"], ["a3"], []]
    assert res == expected
