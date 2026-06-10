from __future__ import annotations

import pytest


def test_simple_concat(make_df, with_morsel_size):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"foo": [4, 5, 6]})
    result = df1.concat(df2)
    assert result.to_pydict() == {"foo": [1, 2, 3, 4, 5, 6]}


def test_concat_schema_mismatch(make_df, with_morsel_size):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"foo": ["4", "5", "6"]})
    with pytest.raises(ValueError):
        df1.concat(df2)


def test_self_concat(make_df, with_morsel_size):
    df = make_df({"foo": [1, 2, 3]})
    assert df.concat(df).to_pydict() == {"foo": [1, 2, 3, 1, 2, 3]}


def test_top_level_concat_multiple(make_df, with_morsel_size):
    import daft

    df1 = make_df({"foo": [1, 2]})
    df2 = make_df({"foo": [3, 4]})
    df3 = make_df({"foo": [5, 6]})
    result = daft.concat([df1, df2, df3])
    assert result.to_pydict() == {"foo": [1, 2, 3, 4, 5, 6]}


def test_top_level_concat_single(make_df, with_morsel_size):
    import daft

    df = make_df({"foo": [1, 2, 3]})
    assert daft.concat([df]).to_pydict() == {"foo": [1, 2, 3]}


def test_top_level_concat_empty():
    import pytest

    import daft

    with pytest.raises(ValueError):
        daft.concat([])
