import daft


def test_to_pylist() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [2, 4, 3, 1]})
    res = df.to_pylist()

    assert res == [{"a": 1, "b": 2}, {"a": 2, "b": 4}, {"a": 3, "b": 3}, {"a": 4, "b": 1}]


def test_to_pylist_with_None() -> None:
    df = daft.from_pydict({"a": [None], "b": [None]})

    assert df.to_pylist() == [{"a": None, "b": None}]
