from typing import Callable

import pytest

import daft
from daft.sql import SQLCatalog


def _assert_sql_raise(sql: str, catalog: SQLCatalog, error_msg: str) -> None:
    with pytest.raises(Exception) as excinfo:
        daft.sql(sql, catalog=catalog).collect()

    assert error_msg in str(excinfo.value)


def _assert_df_op_raise(func: Callable, error_msg: str) -> None:
    with pytest.raises(Exception) as excinfo:
        func()

    assert error_msg in str(excinfo.value)


def test_div_floor():
    df = daft.from_pydict({"A": [1, 2, 3, 4], "B": [1.5, 2.5, 3.5, 4.5], "C": [4, 5, 6, 7]})

    actual1 = daft.sql("SELECT (A // 1) AS div_floor FROM df").collect().to_pydict()
    actual2 = df.select((daft.col("A") // 1).alias("div_floor")).collect().to_pydict()
    expected = {
        "div_floor": [1, 2, 3, 4],
    }
    assert actual1 == actual2 == expected

    actual1 = daft.sql("SELECT (A // B) AS div_floor FROM df").collect().to_pydict()
    actual2 = df.select((daft.col("A") // daft.col("B")).alias("div_floor")).collect().to_pydict()
    expected = {
        "div_floor": [0, 0, 0, 0],
    }
    assert actual1 == actual2 == expected

    actual1 = daft.sql("SELECT (B // A) AS div_floor FROM df").collect().to_pydict()
    actual2 = df.select((daft.col("B") // daft.col("A")).alias("div_floor")).collect().to_pydict()
    expected = {
        "div_floor": [1.0, 1.0, 1.0, 1.0],
    }
    assert actual1 == actual2 == expected

    actual1 = daft.sql("SELECT (C // A) AS div_floor FROM df").collect().to_pydict()
    actual2 = df.select((daft.col("C") // daft.col("A")).alias("div_floor")).collect().to_pydict()
    expected = {
        "div_floor": [4, 2, 2, 1],
    }
    assert actual1 == actual2 == expected


def test_unsupported_div_floor():
    df = daft.from_pydict({"A": [1, 2, 3, 4], "B": [1.5, 2.5, 3.5, 4.5], "C": [True, False, True, True]})

    catalog = SQLCatalog({"df": df})

    _assert_sql_raise(
        "SELECT A // C FROM df", catalog, "TypeError Cannot perform floor divide on types: Int64, Boolean"
    )

    _assert_sql_raise(
        "SELECT C // A FROM df", catalog, "TypeError Cannot perform floor divide on types: Boolean, Int64"
    )

    _assert_sql_raise(
        "SELECT B // C FROM df", catalog, "TypeError Cannot perform floor divide on types: Float64, Boolean"
    )

    _assert_sql_raise(
        "SELECT B // C FROM df", catalog, "TypeError Cannot perform floor divide on types: Float64, Boolean"
    )

    _assert_df_op_raise(
        lambda: df.select(daft.col("A") // daft.col("C")).collect(),
        "TypeError Cannot perform floor divide on types: Int64, Boolean",
    )

    _assert_df_op_raise(
        lambda: df.select(daft.col("C") // daft.col("A")).collect(),
        "TypeError Cannot perform floor divide on types: Boolean, Int64",
    )

    _assert_df_op_raise(
        lambda: df.select(daft.col("B") // daft.col("C")).collect(),
        "TypeError Cannot perform floor divide on types: Float64, Boolean",
    )

    _assert_df_op_raise(
        lambda: df.select(daft.col("C") // daft.col("B")).collect(),
        "TypeError Cannot perform floor divide on types: Boolean, Float64",
    )
