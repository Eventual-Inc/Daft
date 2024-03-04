from __future__ import annotations

import datetime
import math

import pandas as pd
import pytest

import daft
from daft.context import set_execution_config
from tests.conftest import assert_df_equals
from tests.integration.sql.conftest import TEST_TABLE_NAME


@pytest.mark.integration()
def test_sql_create_dataframe_ok(test_db) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    pdf = pd.read_sql_query(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)

    assert_df_equals(df.to_pandas(), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [2, 3, 4])
def test_sql_partitioned_read(test_db, num_partitions) -> None:
    pdf = pd.read_sql_query(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)

    row_size_bytes = daft.from_pandas(pdf).schema().estimate_row_size_bytes()
    num_rows_per_partition = len(pdf) / num_partitions
    set_execution_config(read_sql_partition_size_bytes=math.ceil(row_size_bytes * num_rows_per_partition))

    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME} ORDER BY id", test_db)
    assert df.num_partitions() == num_partitions
    assert_df_equals(df.to_pandas(), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [2, 3, 4])
def test_sql_partitioned_read_with_custom_num_partitions(test_db, num_partitions) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME} ORDER BY id", test_db, num_partitions=num_partitions)
    assert df.num_partitions() == num_partitions
    pdf = pd.read_sql_query(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    assert_df_equals(df.to_pandas(), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize(
    "operator",
    ["<", ">", "=", "!=", ">=", "<="],
)
@pytest.mark.parametrize(
    "column, value",
    [
        ("id", 100),
        ("float_col", 100.0),
        ("string_col", "row_100"),
        ("bool_col", True),
        ("date_col", datetime.date(2021, 1, 1)),
        # TODO(Colin) - ConnectorX parses datetime as pyarrow date64 type, which we currently cast to Python, causing our assertions to fail.
        # One possible solution is to cast date64 into Timestamp("ms") in our from_arrow code.
        # ("date_time_col", datetime.datetime(2020, 1, 1, 10, 0, 0)),
        # TODO(Colin) - Reading time from Postgres is parsed as Time(Nanoseconds), while from MySQL it is parsed as Duration(Microseconds)
        # Need to fix our time comparison code to handle this.
        # ("time_col", datetime.time(10, 0, 0)),
    ],
)
def test_sql_read_with_binary_filter_pushdowns(test_db, column, operator, value) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    pdf = pd.read_sql_query(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)

    if operator == ">":
        df = df.where(df[column] > value)
        pdf = pdf[pdf[column] > value]
    elif operator == "<":
        df = df.where(df[column] < value)
        pdf = pdf[pdf[column] < value]
    elif operator == "=":
        df = df.where(df[column] == value)
        pdf = pdf[pdf[column] == value]
    elif operator == "!=":
        df = df.where(df[column] != value)
        pdf = pdf[pdf[column] != value]
    elif operator == ">=":
        df = df.where(df[column] >= value)
        pdf = pdf[pdf[column] >= value]
    elif operator == "<=":
        df = df.where(df[column] <= value)
        pdf = pdf[pdf[column] <= value]

    assert_df_equals(df.to_pandas(), pdf, sort_key="id")


@pytest.mark.integration()
def test_sql_read_with_is_null_filter_pushdowns(test_db) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.where(df["null_col"].is_null())

    pydict = df.to_pydict()
    assert all(value is None for value in pydict["null_col"])


@pytest.mark.integration()
def test_sql_read_with_not_null_filter_pushdowns(test_db) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.where(df["null_col"].not_null())

    pydict = df.to_pydict()
    assert all(value is not None for value in pydict["null_col"])


@pytest.mark.integration()
def test_sql_read_with_if_else_filter_pushdown(test_db) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.where((df["id"] > 100).if_else(df["float_col"] > 150, df["float_col"] < 50))

    pydict = df.to_pydict()
    assert all(value < 50 or value > 150 for value in pydict["float_col"])


@pytest.mark.integration()
def test_sql_read_with_all_pushdowns(test_db) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df = df.where(~(df["id"] < 1))
    df = df.where(df["string_col"].is_in([f"row_{i}" for i in range(10)]))
    df = df.select(df["id"], df["string_col"])
    df = df.limit(1)

    df = df.collect()
    assert df.column_names == ["id", "string_col"]
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
    df = df.select(df["id"], df["string_col"])

    df = df.collect()
    assert df.column_names == ["id", "string_col"]
    assert len(df) == len(generated_data)


@pytest.mark.integration()
def test_sql_bad_url() -> None:
    with pytest.raises(RuntimeError, match="Failed to execute sql"):
        daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", "bad_url://")
