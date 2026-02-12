from __future__ import annotations

from typing import Any

import pytest

import daft
from daft import DataFrame, DataType, col


def test_filter_missing_column(make_df, valid_data: list[dict[str, Any]]) -> None:
    df = make_df(valid_data)
    with pytest.raises(ValueError):
        df.select("sepal_width").where(df["petal_length"] > 4.8)


def test_drop_na(make_df, missing_value_data: list[dict[str, Any]]) -> None:
    df: DataFrame = make_df(missing_value_data)
    df_len_no_col = len(df.drop_nan().collect())
    assert df_len_no_col == 2

    df: DataFrame = make_df(missing_value_data)
    df_len_col = len(df.drop_nan("sepal_width").collect())
    assert df_len_col == 2


def test_drop_null(make_df, missing_value_data: list[dict[str, Any]]) -> None:
    df: DataFrame = make_df(missing_value_data)
    df_len_no_col = len(df.drop_null().collect())
    assert df_len_no_col == 2

    df: DataFrame = make_df(missing_value_data)
    df_len_col = len(df.drop_null("sepal_length").collect())
    assert df_len_col == 2


def test_filter_sql() -> None:
    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 9, 9]})

    df = df.where("z = 9 AND y > 5").collect().to_pydict()
    expected = {"x": [3], "y": [6], "z": [9]}

    assert df == expected


def test_filter_alias_for_where() -> None:
    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 9, 9]})

    expected = df.where("z = 9 AND y > 5").collect().to_pydict()
    actual = df.filter("z = 9 AND y > 5").collect().to_pydict()

    assert actual == expected


@pytest.mark.parametrize(
    "op,expected",
    [
        (
            "__lt__",
            {
                "id": [1, 2, 3],
                "values": [[1, 2, 3], [4, 5, 6], [1, 2]],
                "threshold": [[1, 2, 4], [5, 6, 7], [1, 2, 3]],
            },
        ),
        ("__eq__", {"id": [4], "values": [[7, 8, 9]], "threshold": [[7, 8, 9]]}),
        (
            "__ne__",
            {
                "id": [1, 2, 3],
                "values": [[1, 2, 3], [4, 5, 6], [1, 2]],
                "threshold": [[1, 2, 4], [5, 6, 7], [1, 2, 3]],
            },
        ),
    ],
)
def test_filter_with_list_column_comparison(op, expected) -> None:
    df = daft.from_pydict(
        {
            "id": [1, 2, 3, 4],
            "values": [[1, 2, 3], [4, 5, 6], [1, 2], [7, 8, 9]],
            "threshold": [[1, 2, 4], [5, 6, 7], [1, 2, 3], [7, 8, 9]],
        }
    )

    predicate = getattr(col("values"), op)(col("threshold"))
    result = df.where(predicate).collect().to_pydict()
    assert result == expected


@pytest.mark.parametrize(
    "op,expected",
    [
        ("__lt__", {"id": [2, 3]}),
        ("__eq__", {"id": [1, 4]}),
        ("__ne__", {"id": [2, 3]}),
        ("__ge__", {"id": [1, 4]}),
    ],
)
def test_filter_with_struct_column_comparison(op, expected) -> None:
    df = daft.from_pydict(
        {
            "id": [1, 2, 3, 4],
            "left_struct": [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, {"x": 3, "y": "c"}, {"x": 4, "y": "d"}],
            "right_struct": [{"x": 1, "y": "a"}, {"x": 2, "y": "c"}, {"x": 4, "y": "c"}, {"x": 4, "y": "d"}],
        }
    )

    df = df.with_column(
        "left_struct", col("left_struct").cast(DataType.struct({"x": DataType.int64(), "y": DataType.string()}))
    )
    df = df.with_column(
        "right_struct", col("right_struct").cast(DataType.struct({"x": DataType.int64(), "y": DataType.string()}))
    )

    predicate = getattr(col("left_struct"), op)(col("right_struct"))
    result = df.where(predicate).select("id").collect().to_pydict()
    assert result == expected


@pytest.mark.parametrize(
    "op,expected",
    [
        ("__lt__", {"id": [1], "values": [[1, 2, 3]], "threshold": [[1, 2, 4]]}),
        ("__eq__", {"id": [4], "values": [[7, 8, 9]], "threshold": [[7, 8, 9]]}),
    ],
)
def test_filter_list_with_null_values(op, expected) -> None:
    df = daft.from_pydict(
        {
            "id": [1, 2, 3, 4],
            "values": [[1, 2, 3], None, [5, 6], [7, 8, 9]],
            "threshold": [[1, 2, 4], [5, 6, 7], None, [7, 8, 9]],
        }
    )

    predicate = getattr(col("values"), op)(col("threshold"))
    result = df.where(predicate).collect().to_pydict()
    assert result == expected
