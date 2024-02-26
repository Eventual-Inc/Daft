from __future__ import annotations

import pytest

import daft
from tests.conftest import assert_df_equals
from tests.integration.sql.conftest import (
    NUM_ROWS_PER_PARTITION,
    NUM_TEST_ROWS,
    TEST_TABLE_NAME,
)


@pytest.mark.integration()
def test_sql_create_dataframe_ok(test_db, generated_data) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)

    assert_df_equals(df.to_pandas(), generated_data, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2, 3])
def test_sql_partitioned_read(test_db, num_partitions) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME} LIMIT {NUM_ROWS_PER_PARTITION * num_partitions}", test_db)
    assert df.num_partitions() == num_partitions
    df = df.collect()
    assert len(df) == NUM_ROWS_PER_PARTITION * num_partitions

    # test with a number of rows that is not a multiple of NUM_ROWS_PER_PARTITION
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME} LIMIT {NUM_ROWS_PER_PARTITION * num_partitions + 1}", test_db)
    assert df.num_partitions() == num_partitions + 1
    df = df.collect()
    assert len(df) == NUM_ROWS_PER_PARTITION * num_partitions + 1


@pytest.mark.integration()
@pytest.mark.parametrize(
    "column,operator,value,expected_length",
    [
        ("id", ">", 100, 99),
        ("id", "<=", 100, 101),
        ("sepal_length", "<", 100.0, 100),
        ("sepal_length", ">=", 100.0, 100),
        ("variety", "=", "setosa", 50),
        ("variety", "!=", "setosa", 100),
        ("variety", "is_null", None, 50),
        ("variety", "not_null", None, 150),
        ("variety", "is_in", ["setosa", "versicolor"], 100),
        ("id", "is_in", [1, 2, 3], 3),
    ],
)
def test_sql_read_with_filter_pushdowns(test_db, column, operator, value, expected_length) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)

    if operator == ">":
        df = df.where(df[column] > value)
    elif operator == "<":
        df = df.where(df[column] < value)
    elif operator == "=":
        df = df.where(df[column] == value)
    elif operator == "!=":
        df = df.where(df[column] != value)
    elif operator == ">=":
        df = df.where(df[column] >= value)
    elif operator == "<=":
        df = df.where(df[column] <= value)
    elif operator == "is_null":
        df = df.where(df[column].is_null())
    elif operator == "not_null":
        df = df.where(df[column].not_null())
    elif operator == "is_in":
        df = df.where(df[column].is_in(value))

    df = df.collect()
    assert len(df) == expected_length


@pytest.mark.integration()
def test_sql_read_with_if_else_filter_pushdown(test_db) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.where(df["variety"] == (df["sepal_length"] >= 125).if_else("virginica", "setosa"))

    df = df.collect()
    assert len(df) == 75


@pytest.mark.integration()
def test_sql_read_with_all_pushdowns(test_db) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.where(df["sepal_length"] > 100)
    df = df.select(df["sepal_length"], df["variety"])
    df = df.limit(50)

    df = df.collect()
    assert df.column_names == ["sepal_length", "variety"]
    assert len(df) == 50


@pytest.mark.integration()
@pytest.mark.parametrize("limit", [0, 1, 10, 100, 200])
def test_sql_read_with_limit_pushdown(test_db, limit) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.limit(limit)

    df = df.collect()
    assert len(df) == limit


@pytest.mark.integration()
def test_sql_read_with_projection_pushdown(test_db) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.select(df["sepal_length"], df["variety"])

    df = df.collect()
    assert df.column_names == ["sepal_length", "variety"]
    assert len(df) == NUM_TEST_ROWS


@pytest.mark.integration()
def test_sql_bad_url() -> None:
    with pytest.raises(RuntimeError, match="Failed to execute sql"):
        daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", "bad_url://")
