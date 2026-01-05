from __future__ import annotations

import pytest


def test_simple_concat(make_df, with_morsel_size):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"foo": [4, 5, 6]})
    result = df1.concat(df2)
    result_dict = result.to_pydict()
    # Concat no longer has deterministic ordering, so we sort before comparing
    assert sorted(result_dict["foo"]) == [1, 2, 3, 4, 5, 6]


def test_concat_schema_mismatch(make_df, with_morsel_size):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"foo": ["4", "5", "6"]})
    with pytest.raises(ValueError):
        df1.concat(df2)


def test_self_concat(make_df, with_morsel_size):
    df = make_df({"foo": [1, 2, 3]})
    result_dict = df.concat(df).to_pydict()
    # Concat no longer has deterministic ordering, so we sort before comparing
    assert sorted(result_dict["foo"]) == [1, 1, 2, 2, 3, 3]
