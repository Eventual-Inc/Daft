from __future__ import annotations

import pytest

from daft import context

pytestmark = pytest.mark.skipif(
    context.get_context().daft_execution_config.enable_native_executor is True,
    reason="Native executor fails for these tests",
)


def test_simple_concat(make_df):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"foo": [4, 5, 6]})
    result = df1.concat(df2)
    assert result.to_pydict() == {"foo": [1, 2, 3, 4, 5, 6]}


def test_concat_schema_mismatch(make_df):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"foo": ["4", "5", "6"]})
    with pytest.raises(ValueError):
        df1.concat(df2)


def test_self_concat(make_df):
    df = make_df({"foo": [1, 2, 3]})
    assert df.concat(df).to_pydict() == {"foo": [1, 2, 3, 1, 2, 3]}
