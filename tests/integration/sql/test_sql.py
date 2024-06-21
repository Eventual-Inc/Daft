from __future__ import annotations

import datetime
import math

import pandas as pd
import pytest
import sqlalchemy

import daft
from daft.context import set_execution_config
from tests.conftest import assert_df_equals
from tests.integration.sql.conftest import TEST_TABLE_NAME


@pytest.fixture(scope="session")
def pdf(test_db):
    return pd.read_sql_query(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)


@pytest.mark.integration()
def test_sql_show(test_db) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    df.show()


@pytest.mark.integration()
def test_sql_create_dataframe_ok(test_db, pdf) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [2, 3, 4])
def test_sql_partitioned_read(test_db, num_partitions, pdf) -> None:
    row_size_bytes = daft.from_pandas(pdf).schema().estimate_row_size_bytes()
    num_rows_per_partition = len(pdf) / num_partitions
    set_execution_config(read_sql_partition_size_bytes=math.ceil(row_size_bytes * num_rows_per_partition))

    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, partition_col="id")
    assert df.num_partitions() == num_partitions
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2, 3, 4])
@pytest.mark.parametrize("partition_col", ["id", "float_col", "date_col", "date_time_col"])
def test_sql_partitioned_read_with_custom_num_partitions_and_partition_col(
    test_db, num_partitions, partition_col, pdf
) -> None:
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}", test_db, partition_col=partition_col, num_partitions=num_partitions
    )
    assert df.num_partitions() == num_partitions
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2, 3, 4])
def test_sql_partitioned_read_with_non_uniformly_distributed_column(test_db, num_partitions, pdf) -> None:
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db,
        partition_col="non_uniformly_distributed_col",
        num_partitions=num_partitions,
    )
    assert df.num_partitions() == num_partitions
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("partition_col", ["string_col", "time_col", "null_col"])
def test_sql_partitioned_read_with_non_partionable_column(test_db, partition_col) -> None:
    with pytest.raises(ValueError, match="Failed to get partition bounds"):
        df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, partition_col=partition_col, num_partitions=2)
        df = df.collect()


@pytest.mark.integration()
def test_sql_read_with_partition_num_without_partition_col(test_db) -> None:
    with pytest.raises(ValueError, match="Failed to execute sql"):
        daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, num_partitions=2)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "operator",
    ["<", ">", "=", "!=", ">=", "<="],
)
@pytest.mark.parametrize(
    "column, value",
    [
        ("id", 100),
        ("float_col", 100.0123),
        ("string_col", "row_100"),
        ("bool_col", True),
        ("date_col", datetime.date(2021, 1, 1)),
        ("date_time_col", datetime.datetime(2020, 1, 1, 10, 0, 0)),
    ],
)
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_binary_filter_pushdowns(test_db, column, operator, value, num_partitions, pdf) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, partition_col="id", num_partitions=num_partitions)

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

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_is_null_filter_pushdowns(test_db, num_partitions, pdf) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, partition_col="id", num_partitions=num_partitions)
    df = df.where(df["null_col"].is_null())

    pdf = pdf[pdf["null_col"].isnull()]

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_not_null_filter_pushdowns(test_db, num_partitions, pdf) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, partition_col="id", num_partitions=num_partitions)
    df = df.where(df["null_col"].not_null())

    pdf = pdf[pdf["null_col"].notnull()]

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_if_else_filter_pushdown(test_db, num_partitions, pdf) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, partition_col="id", num_partitions=num_partitions)
    df = df.where((df["id"] > 100).if_else(df["float_col"] > 150, df["float_col"] < 50))

    pdf = pdf[(pdf["id"] > 100) & (pdf["float_col"] > 150) | (pdf["float_col"] < 50)]

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_is_in_filter_pushdown(test_db, num_partitions, pdf) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, partition_col="id", num_partitions=num_partitions)
    df = df.where(df["id"].is_in([1, 2, 3]))

    pdf = pdf[pdf["id"].isin([1, 2, 3])]
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_all_pushdowns(test_db, num_partitions) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, partition_col="id", num_partitions=num_partitions)
    df = df.where(~(df["id"] < 1))
    df = df.where(df["string_col"].is_in([f"row_{i}" for i in range(10)]))
    df = df.select(df["id"], df["float_col"], df["string_col"])
    df = df.limit(5)

    df = df.collect()
    assert len(df) == 5
    assert df.column_names == ["id", "float_col", "string_col"]

    pydict = df.to_pydict()
    assert all(i >= 1 for i in pydict["id"])
    assert all(s in [f"row_{i}" for i in range(10)] for s in pydict["string_col"])


@pytest.mark.integration()
@pytest.mark.parametrize("limit", [0, 1, 10, 100, 200])
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_limit_pushdown(test_db, limit, num_partitions) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, partition_col="id", num_partitions=num_partitions)
    df = df.limit(limit)

    df = df.collect()
    assert len(df) == limit


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_sql_read_with_projection_pushdown(test_db, generated_data, num_partitions) -> None:
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db, partition_col="id", num_partitions=num_partitions)
    df = df.select(df["id"], df["string_col"])

    df = df.collect()
    assert df.column_names == ["id", "string_col"]
    assert len(df) == len(generated_data)


@pytest.mark.integration()
def test_sql_bad_url() -> None:
    with pytest.raises(RuntimeError, match="Failed to execute sql"):
        daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", "bad_url://")


@pytest.mark.integration()
def test_sql_connection_factory_ok(test_db, pdf) -> None:
    def create_conn():
        return sqlalchemy.create_engine(test_db).connect()

    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", create_conn)
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
def test_sql_bad_connection_factory() -> None:
    with pytest.raises(ValueError):
        daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", lambda: "bad_conn")


@pytest.mark.integration()
def test_sql_unsupported_dialect() -> None:
    with pytest.raises(ValueError, match="Unsupported dialect"):
        daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", "sqlheavy://user:password@localhost:5432/db")
