from __future__ import annotations

import pytest

import daft


def test_select_dataframe_missing_col(valid_data: list[dict[str, float]]) -> None:
    df = daft.from_pylist(valid_data)

    with pytest.raises(ValueError):
        df = df.select("foo", "sepal_length")


def test_select_dataframe(valid_data: list[dict[str, float]]) -> None:
    df = daft.from_pylist(valid_data)
    df = df.select("sepal_length", "sepal_width")
    assert df.column_names == ["sepal_length", "sepal_width"]


def test_multiple_select_same_col(valid_data: list[dict[str, float]]):
    df = daft.from_pylist(valid_data)
    df = df.select(df["sepal_length"], df["sepal_length"].alias("sepal_length_2"))
    pdf = df.to_pandas()
    assert len(pdf.columns) == 2
    assert pdf.columns.to_list() == ["sepal_length", "sepal_length_2"]
