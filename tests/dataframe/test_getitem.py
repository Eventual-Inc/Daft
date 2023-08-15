from __future__ import annotations

import pytest

import daft
from daft import DataFrame


def test_dataframe_getitem_single(valid_data: list[dict[str, float]]) -> None:
    df = daft.from_pylist(valid_data)
    expanded_df = df.with_column("foo", df["sepal_length"] + df["sepal_width"])
    # TODO(jay): Test that the expression with name "foo" is equal to the expected expression, except for the IDs of the columns

    assert expanded_df["foo"]._is_column()
    assert expanded_df.column_names == df.column_names + ["foo"]
    assert df.select(df["sepal_length"]).column_names == ["sepal_length"]


def test_dataframe_getitem_single_bad(valid_data: list[dict[str, float]]) -> None:
    df = daft.from_pylist(valid_data)
    with pytest.raises(ValueError, match="not found"):
        df["foo"]

    with pytest.raises(ValueError, match="bounds"):
        df[-100]

    with pytest.raises(ValueError, match="bounds"):
        df[100]


def test_dataframe_getitem_multiple_bad(valid_data: list[dict[str, float]]) -> None:
    df = daft.from_pylist(valid_data)
    with pytest.raises(ValueError, match="not found"):
        df["foo", "bar"]

    with pytest.raises(ValueError, match="bounds"):
        df[-100, -200]

    with pytest.raises(ValueError, match="bounds"):
        df[100, 200]

    with pytest.raises(ValueError, match="indexing type"):
        df[[{"a": 1}]]

    class A:
        ...

    with pytest.raises(ValueError, match="indexing type"):
        df[A()]


def test_dataframe_getitem_multiple(valid_data: list[dict[str, float]]) -> None:
    df = daft.from_pylist(valid_data)
    expanded_df = df.with_column("foo", sum(df["sepal_length", "sepal_width"].columns))
    # TODO(jay): Test that the expression with name "foo" is equal to the expected expression, except for the IDs of the columns
    assert expanded_df.column_names == df.column_names + ["foo"]
    assert isinstance(df["sepal_length", "sepal_width"], DataFrame)
    assert df["sepal_length", "sepal_width"].column_names == ["sepal_length", "sepal_width"]


def test_dataframe_getitem_slice(valid_data: list[dict[str, float]]) -> None:
    df = daft.from_pylist(valid_data)
    slice_df = df[:]
    assert df.column_names == slice_df.column_names


def test_dataframe_getitem_slice_rev(valid_data: list[dict[str, float]]) -> None:
    df = daft.from_pylist(valid_data)
    slice_df = df[::-1]
    assert df.column_names == slice_df.column_names[::-1]
