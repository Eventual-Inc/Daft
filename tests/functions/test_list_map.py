from __future__ import annotations

import daft


def test_list_map():
    items = [["a", "b", "a"], ["b", "c", "b", "c"]]
    df = daft.from_pydict({"letters": items})
    expected = [["A", "B", "A"], ["B", "C", "B", "C"]]
    actual = df.select(daft.functions.list_map(daft.col("letters"), daft.functions.upper(daft.element()))).to_pydict()[
        "letters"
    ]
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
        daft.functions.list_map(daft.col("letters"), daft.element().apply(lambda s: s.upper(), return_dtype=str))
    ).to_pydict()["letters"]
    assert actual == expected


def test_map_chaining():
    df = daft.from_pydict({"numbers": [[1, 2, 3], [4, 5, 6, 7]]})
    actual = df.select(
        daft.functions.list_map(daft.functions.list_map(daft.col("numbers"), daft.element() * 2), daft.element() * 4)
    ).to_pydict()["numbers"]

    expected = [[8, 16, 24], [32, 40, 48, 56]]

    assert actual == expected


def test_map_nested():
    df = daft.from_pydict({"sentences": [["this is a test", "another test"]]})

    words = (
        df.select(
            daft.functions.list_map(daft.col("sentences"), daft.functions.split(daft.element(), " ")).alias("words")
        )
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

    res = df.select(daft.functions.list_map(daft.col("structs"), daft.functions.get(daft.element(), "a"))).to_pydict()[
        "a"
    ]

    expected = [["a1", "a2"], ["a3"], []]
    assert res == expected
