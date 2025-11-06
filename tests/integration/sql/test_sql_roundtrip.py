from __future__ import annotations

import datetime
import math

import pandas as pd
import pytest

import daft
from tests.conftest import assert_df_equals
from tests.integration.sql.conftest import TEST_TABLE_NAME


@pytest.fixture(scope="session")
def pdf_via_write_sql(test_db_via_write_sql):
    """Get pandas DataFrame from database populated via write_sql."""
    return pd.read_sql_query(f"SELECT * FROM {TEST_TABLE_NAME}", test_db_via_write_sql)


@pytest.mark.integration()
def test_roundtrip_basic_read(test_db_via_write_sql, pdf_via_write_sql, generated_data) -> None:
    """Test that basic read_sql works on data written by write_sql."""
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db_via_write_sql)

    # Should match the original generated data
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), generated_data, sort_key="id")

    # Should also match what pandas reads from the database
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf_via_write_sql, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [2, 3, 4])
@pytest.mark.parametrize("partition_bound_strategy", ["min-max", "percentile"])
def test_roundtrip_partitioned_read(
    test_db_via_write_sql, num_partitions, partition_bound_strategy, pdf_via_write_sql
) -> None:
    """Test that partitioned read_sql works on data written by write_sql."""
    row_size_bytes = daft.from_pandas(pdf_via_write_sql).schema().estimate_row_size_bytes()
    num_rows_per_partition = len(pdf_via_write_sql) / num_partitions

    with daft.execution_config_ctx(
        read_sql_partition_size_bytes=math.ceil(row_size_bytes * num_rows_per_partition),
        scan_tasks_min_size_bytes=0,
        scan_tasks_max_size_bytes=0,
    ):
        df = daft.read_sql(
            f"SELECT * FROM {TEST_TABLE_NAME}",
            test_db_via_write_sql,
            partition_col="id",
            partition_bound_strategy=partition_bound_strategy,
        )
        assert df.num_partitions() == num_partitions
        assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf_via_write_sql, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2, 3, 4])
@pytest.mark.parametrize("partition_col", ["id", "float_col", "date_col", "date_time_col"])
@pytest.mark.parametrize("partition_bound_strategy", ["min-max", "percentile"])
def test_roundtrip_partitioned_read_with_custom_partition_col(
    test_db_via_write_sql, num_partitions, partition_col, partition_bound_strategy, pdf_via_write_sql
) -> None:
    """Test partitioned reads with various partition columns on write_sql data."""
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=0,
        scan_tasks_max_size_bytes=0,
    ):
        df = daft.read_sql(
            f"SELECT * FROM {TEST_TABLE_NAME}",
            test_db_via_write_sql,
            partition_col=partition_col,
            num_partitions=num_partitions,
            partition_bound_strategy=partition_bound_strategy,
        )
        assert df.num_partitions() == num_partitions
        assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf_via_write_sql, sort_key="id")


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
def test_roundtrip_read_with_binary_filter_pushdowns(
    test_db_via_write_sql, column, operator, value, num_partitions, pdf_via_write_sql
) -> None:
    """Test that filter pushdowns work on data written by write_sql."""
    # Skip invalid comparisons for bool_col
    if column == "bool_col" and operator not in ("=", "!="):
        pytest.skip(f"Operator {operator} not valid for bool_col")

    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db_via_write_sql,
        partition_col="id",
        num_partitions=num_partitions,
    )

    if operator == ">":
        df = df.where(df[column] > value)
        pdf = pdf_via_write_sql[pdf_via_write_sql[column] > value]
    elif operator == "<":
        df = df.where(df[column] < value)
        pdf = pdf_via_write_sql[pdf_via_write_sql[column] < value]
    elif operator == "=":
        df = df.where(df[column] == value)
        pdf = pdf_via_write_sql[pdf_via_write_sql[column] == value]
    elif operator == "!=":
        df = df.where(df[column] != value)
        pdf = pdf_via_write_sql[pdf_via_write_sql[column] != value]
    elif operator == ">=":
        df = df.where(df[column] >= value)
        pdf = pdf_via_write_sql[pdf_via_write_sql[column] >= value]
    elif operator == "<=":
        df = df.where(df[column] <= value)
        pdf = pdf_via_write_sql[pdf_via_write_sql[column] <= value]

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_roundtrip_read_with_is_null_filter_pushdowns(test_db_via_write_sql, num_partitions, pdf_via_write_sql) -> None:
    """Test that is_null filter pushdowns work on data written by write_sql."""
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db_via_write_sql,
        partition_col="id",
        num_partitions=num_partitions,
    )
    df = df.where(df["null_col"].is_null())

    pdf = pdf_via_write_sql[pdf_via_write_sql["null_col"].isnull()]

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_roundtrip_read_with_not_null_filter_pushdowns(
    test_db_via_write_sql, num_partitions, pdf_via_write_sql
) -> None:
    """Test that not_null filter pushdowns work on data written by write_sql."""
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db_via_write_sql,
        partition_col="id",
        num_partitions=num_partitions,
    )
    df = df.where(df["null_col"].not_null())

    pdf = pdf_via_write_sql[pdf_via_write_sql["null_col"].notnull()]

    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_roundtrip_read_with_is_in_filter_pushdown(test_db_via_write_sql, num_partitions, pdf_via_write_sql) -> None:
    """Test that is_in filter pushdown works on data written by write_sql."""
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db_via_write_sql,
        partition_col="id",
        num_partitions=num_partitions,
    )
    df = df.where(df["id"].is_in([1, 2, 3]))

    pdf = pdf_via_write_sql[pdf_via_write_sql["id"].isin([1, 2, 3])]
    assert_df_equals(df.to_pandas(coerce_temporal_nanoseconds=True), pdf, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("limit", [0, 1, 10, 100, 200])
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_roundtrip_read_with_limit_pushdown(test_db_via_write_sql, limit, num_partitions) -> None:
    """Test that limit pushdown works on data written by write_sql."""
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db_via_write_sql,
        partition_col="id",
        num_partitions=num_partitions,
    )
    df = df.limit(limit)

    df = df.collect()
    assert len(df) == limit


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_roundtrip_read_with_projection_pushdown(test_db_via_write_sql, generated_data, num_partitions) -> None:
    """Test that projection pushdown works on data written by write_sql."""
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db_via_write_sql,
        partition_col="id",
        num_partitions=num_partitions,
    )
    df = df.select(df["id"], df["string_col"])

    df = df.collect()
    assert df.column_names == ["id", "string_col"]
    assert len(df) == len(generated_data)


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [1, 2])
def test_roundtrip_read_with_all_pushdowns(test_db_via_write_sql, num_partitions) -> None:
    """Test that all pushdowns work together on data written by write_sql."""
    df = daft.read_sql(
        f"SELECT * FROM {TEST_TABLE_NAME}",
        test_db_via_write_sql,
        partition_col="id",
        num_partitions=num_partitions,
    )
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
def test_roundtrip_show(test_db_via_write_sql) -> None:
    """Test that show() works on data written by write_sql."""
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db_via_write_sql)
    df.show()


@pytest.mark.integration()
def test_roundtrip_data_integrity(test_db_via_write_sql, generated_data) -> None:
    """Comprehensive test verifying complete data integrity after write_sql -> read_sql roundtrip.

    This test ensures that:
    1. All rows are preserved
    2. All columns are preserved
    3. All data types are preserved
    4. All values (including nulls) are preserved exactly
    """
    # Read back data written by write_sql
    df = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db_via_write_sql)
    read_pdf = df.to_pandas(coerce_temporal_nanoseconds=True)

    # Verify row count
    assert len(read_pdf) == len(generated_data), "Row count mismatch after roundtrip"

    # Verify column names
    assert set(read_pdf.columns) == set(generated_data.columns), "Column names mismatch after roundtrip"

    # Verify data integrity
    assert_df_equals(read_pdf, generated_data, sort_key="id")

    # Verify null handling
    original_null_count = generated_data["null_col"].isna().sum()
    read_null_count = read_pdf["null_col"].isna().sum()
    assert original_null_count == read_null_count, "Null count mismatch in null_col after roundtrip"


@pytest.mark.integration()
def test_roundtrip_comparison_with_sqlalchemy_setup(test_db, test_db_via_write_sql, pdf) -> None:
    """Compare read results from SQLAlchemy-populated DB vs write_sql-populated DB.

    This is the ultimate validation test: it proves that write_sql produces
    databases that are functionally identical to those created by SQLAlchemy's to_sql.
    """
    # Only run this test if both fixtures use the same database URL
    # (they are parameterized independently, so we only test matching URLs)
    if test_db != test_db_via_write_sql:
        pytest.skip("Skipping - database URLs don't match for this parameter combination")

    # Read from SQLAlchemy-populated database
    df_sqlalchemy = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db)
    pdf_sqlalchemy = df_sqlalchemy.to_pandas(coerce_temporal_nanoseconds=True)

    # Read from write_sql-populated database
    df_write_sql = daft.read_sql(f"SELECT * FROM {TEST_TABLE_NAME}", test_db_via_write_sql)
    pdf_write_sql = df_write_sql.to_pandas(coerce_temporal_nanoseconds=True)

    # Both should match the original pandas DataFrame
    assert_df_equals(pdf_sqlalchemy, pdf, sort_key="id")
    assert_df_equals(pdf_write_sql, pdf, sort_key="id")

    # Most importantly: they should match each other
    assert_df_equals(pdf_sqlalchemy, pdf_write_sql, sort_key="id")
