from __future__ import annotations

import pytest
from daft import col


def test_select_dataframe_missing_col(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)

    with pytest.raises(ValueError):
        df = df.select("foo", "sepal_length")


def test_select_dataframe(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.select("sepal_length", "sepal_width")
    assert df.column_names == ["sepal_length", "sepal_width"]


def test_multiple_select_same_col(make_df, valid_data: list[dict[str, float]]):
    df = make_df(valid_data)
    df = df.select(df["sepal_length"], df["sepal_length"].alias("sepal_length_2"))
    pdf = df.to_pandas()
    assert len(pdf.columns) == 2
    assert pdf.columns.to_list() == ["sepal_length", "sepal_length_2"]


def test_select_ordering(make_df, valid_data: list[dict[str, float]]):
    df = make_df(valid_data)
    df = df.select(
        df["variety"], df["petal_length"].alias("foo"), df["sepal_length"], df["sepal_width"], df["petal_width"]
    )
    df = df.collect()
    assert df.column_names == ["variety", "foo", "sepal_length", "sepal_width", "petal_width"]


def test_select_with_dict_single_arg(make_df, valid_data: list[dict[str, float]]):
    """Test that select works with a dictionary as the only argument."""
    df = make_df(valid_data)
    df = df.select({"new_col": col("sepal_length"), "another_col": col("sepal_width")})
    assert df.column_names == ["new_col", "another_col"]


def test_select_with_dict_multiple_dicts_error(make_df, valid_data: list[dict[str, float]]):
    """Test that select raises an error when multiple dictionaries are provided."""
    df = make_df(valid_data)
    with pytest.raises(ValueError, match="If using a dictionary with select, it must be the only argument"):
        df = df.select({"a": col("sepal_length")}, {"b": col("sepal_width")})


def test_select_with_dict_and_other_args_error(make_df, valid_data: list[dict[str, float]]):
    """Test that select raises an error when a dictionary is mixed with other arguments."""
    df = make_df(valid_data)
    with pytest.raises(ValueError, match="If using a dictionary with select, it must be the only argument"):
        df = df.select({"a": col("sepal_length")}, "sepal_width")


def test_select_with_dict_and_expression_error(make_df, valid_data: list[dict[str, float]]):
    """Test that select raises an error when a dictionary is mixed with expressions."""
    df = make_df(valid_data)
    with pytest.raises(ValueError, match="If using a dictionary with select, it must be the only argument"):
        df = df.select({"a": col("sepal_length")}, col("sepal_width"))


def test_select_with_dict_complex_expressions(make_df, valid_data: list[dict[str, float]]):
    """Test that select works with complex expressions in the dictionary."""
    df = make_df(valid_data)
    df = df.select({
        "length_sum": col("sepal_length") + col("petal_length"),
        "width_ratio": col("sepal_width") / col("petal_width"),
        "original_col": col("variety")
    })
    assert df.column_names == ["length_sum", "width_ratio", "original_col"]


def test_select_with_dict_alias_behavior(make_df, valid_data: list[dict[str, float]]):
    """Test that select with dictionary properly handles aliases."""
    df = make_df(valid_data)
    df = df.select({
        "renamed_length": col("sepal_length").alias("should_be_ignored"),
        "renamed_width": col("sepal_width")
    })
    # The dictionary key should override any alias in the expression
    assert df.column_names == ["renamed_length", "renamed_width"]


