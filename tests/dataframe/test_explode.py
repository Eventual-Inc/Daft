from __future__ import annotations

import pyarrow as pa
import pytest

from daft.expressions import col


@pytest.mark.parametrize(
    "data",
    [
        pa.array([[1, 2], [3, 4], None, []], type=pa.list_(pa.int64())),
        pa.array([[1, 2], [3, 4], None, []], type=pa.large_list(pa.int64())),
    ],
)
def test_explode(make_df, data):
    df = make_df({"nested": data, "sidecar": ["a", "b", "c", "d"]})
    df = df.explode(col("nested"))
    assert df.to_pydict() == {"nested": [1, 2, 3, 4, None, None], "sidecar": ["a", "a", "b", "b", "c", "d"]}


@pytest.mark.parametrize(
    "data",
    [
        pa.array([[1, 2], [3, 4], None, []], type=pa.list_(pa.int64())),
        pa.array([[1, 2], [3, 4], None, []], type=pa.large_list(pa.int64())),
    ],
)
def test_explode_multiple_cols(make_df, data):
    df = make_df({"nested": data, "nested2": data, "sidecar": ["a", "b", "c", "d"]})
    df = df.explode(col("nested"), col("nested2"))
    assert df.to_pydict() == {
        "nested": [1, 2, 3, 4, None, None],
        "nested2": [1, 2, 3, 4, None, None],
        "sidecar": ["a", "a", "b", "b", "c", "d"],
    }


def test_explode_bad_col_type(make_df):
    df = make_df({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="cannot be exploded"):
        df = df.explode(col("a"))
