from __future__ import annotations

import math

import pytest

import daft
from daft.context import set_execution_config
from tests.conftest import assert_df_equals
from tests.integration.sql.conftest import TEST_TABLE_NAME


@pytest.mark.integration()
def test_sql_create_dataframe_ok(test_db, generated_data) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)

    assert_df_equals(df.to_pandas(), generated_data, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [2, 3, 4])
def test_sql_partitioned_read(test_db, num_partitions, generated_data) -> None:
    row_size_bytes = daft.from_pandas(generated_data).schema().estimate_row_size_bytes()
    num_rows_per_partition = len(generated_data) // num_partitions
    limit = num_rows_per_partition * num_partitions
    set_execution_config(read_sql_partition_size_bytes=math.ceil(row_size_bytes * num_rows_per_partition))

    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME} LIMIT {limit}", test_db)
    assert df.num_partitions() == num_partitions
    df = df.collect()
    assert len(df) == limit


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [2, 3, 4])
def test_sql_partitioned_read_with_custom_num_partitions(test_db, num_partitions, generated_data) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, num_partitions=num_partitions)
    assert df.num_partitions() == num_partitions
    df = df.collect()
    assert len(df) == len(generated_data)


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
def test_sql_read_with_if_else_filter_pushdown(test_db, generated_data) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.where(df["variety"] == (df["id"] == -1).if_else("virginica", "setosa"))

    df = df.collect()
    assert len(df) == len(generated_data) // 4


@pytest.mark.integration()
def test_sql_read_with_all_pushdowns(test_db) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.where(~(df["sepal_length"] > 1))
    df = df.select(df["sepal_length"], df["variety"])
    df = df.limit(1)

    df = df.collect()
    assert df.column_names == ["sepal_length", "variety"]
    assert len(df) == 1


@pytest.mark.integration()
@pytest.mark.parametrize("limit", [0, 1, 10, 100, 200])
def test_sql_read_with_limit_pushdown(test_db, limit) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.limit(limit)

    df = df.collect()
    assert len(df) == limit


@pytest.mark.integration()
def test_sql_read_with_projection_pushdown(test_db, generated_data) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.select(df["sepal_length"], df["variety"])

    df = df.collect()
    assert df.column_names == ["sepal_length", "variety"]
    assert len(df) == len(generated_data)


@pytest.mark.integration()
def test_sql_bad_url() -> None:
    with pytest.raises(RuntimeError, match="Failed to execute sql"):
        daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", "bad_url://")
