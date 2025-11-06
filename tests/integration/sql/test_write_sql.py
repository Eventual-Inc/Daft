from __future__ import annotations

import datetime

import pandas as pd
import pytest
import sqlalchemy

import daft
from tests.conftest import assert_df_equals
from tests.integration.sql.conftest import WRITE_TEST_TABLE_NAME


@pytest.mark.integration()
def test_write_sql_basic(empty_test_db_for_write, generated_data) -> None:
    """Test basic write_sql functionality and verify metadata."""
    df = daft.from_pandas(generated_data)
    result_df = df.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite")
    result_df = result_df.collect()

    # Verify metadata
    result_dict = result_df.to_pydict()
    assert result_dict["table_name"][0] == WRITE_TEST_TABLE_NAME
    assert result_dict["rows_written"][0] == len(generated_data)
    assert result_dict["bytes_written"][0] > 0

    # Verify data was written correctly by reading it back
    read_df = daft.read_sql(f"SELECT * FROM {WRITE_TEST_TABLE_NAME}", empty_test_db_for_write)
    assert_df_equals(read_df.to_pandas(coerce_temporal_nanoseconds=True), generated_data, sort_key="id")


@pytest.mark.integration()
def test_write_sql_overwrite_mode(empty_test_db_for_write, generated_data) -> None:
    """Test that overwrite mode replaces existing data."""
    # First write
    df1 = daft.from_pandas(generated_data)
    result1 = df1.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite")
    result1.collect()

    # Create different data
    new_data = pd.DataFrame(
        {
            "id": [1000, 1001, 1002],
            "float_col": [1.0, 2.0, 3.0],
            "string_col": ["new_1", "new_2", "new_3"],
            "bool_col": [True, False, True],
            "date_col": [datetime.date(2025, 1, 1)] * 3,
            "date_time_col": [datetime.datetime(2025, 1, 1, 12, 0, 0)] * 3,
            "null_col": [None, "not_null", None],
            "non_uniformly_distributed_col": [99, 99, 99],
        }
    )

    # Overwrite with new data
    df2 = daft.from_pandas(new_data)
    result2 = df2.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite")
    result2.collect()

    # Verify only new data exists
    read_df = daft.read_sql(f"SELECT * FROM {WRITE_TEST_TABLE_NAME}", empty_test_db_for_write)
    read_pdf = read_df.to_pandas(coerce_temporal_nanoseconds=True)
    assert len(read_pdf) == len(new_data)
    assert_df_equals(read_pdf, new_data, sort_key="id")


@pytest.mark.integration()
def test_write_sql_append_mode(empty_test_db_for_write, generated_data) -> None:
    """Test that append mode adds data to existing table."""
    # First write with overwrite to create table
    df1 = daft.from_pandas(generated_data)
    result1 = df1.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite")
    result1.collect()

    # Create additional data with different IDs to avoid conflicts
    additional_data = pd.DataFrame(
        {
            "id": [2000, 2001, 2002],
            "float_col": [10.0, 20.0, 30.0],
            "string_col": ["append_1", "append_2", "append_3"],
            "bool_col": [True, False, True],
            "date_col": [datetime.date(2025, 6, 1)] * 3,
            "date_time_col": [datetime.datetime(2025, 6, 1, 12, 0, 0)] * 3,
            "null_col": [None, "appended", None],
            "non_uniformly_distributed_col": [88, 88, 88],
        }
    )

    # Append new data
    df2 = daft.from_pandas(additional_data)
    result2 = df2.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="append")
    result2.collect()

    # Verify both datasets exist
    read_df = daft.read_sql(f"SELECT * FROM {WRITE_TEST_TABLE_NAME}", empty_test_db_for_write)
    read_pdf = read_df.to_pandas(coerce_temporal_nanoseconds=True)
    assert len(read_pdf) == len(generated_data) + len(additional_data)

    # Verify original data is still there
    original_ids = set(generated_data["id"].tolist())
    read_ids = set(read_pdf["id"].tolist())
    assert original_ids.issubset(read_ids)

    # Verify appended data is there
    appended_ids = set(additional_data["id"].tolist())
    assert appended_ids.issubset(read_ids)


@pytest.mark.integration()
def test_write_sql_data_type_preservation(empty_test_db_for_write, generated_data) -> None:
    """Test that all column data types are preserved correctly."""
    df = daft.from_pandas(generated_data)
    result_df = df.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite")
    result_df.collect()

    # Read back and verify data types
    read_df = daft.read_sql(f"SELECT * FROM {WRITE_TEST_TABLE_NAME}", empty_test_db_for_write)
    read_pdf = read_df.to_pandas(coerce_temporal_nanoseconds=True)

    # Check all data matches
    assert_df_equals(read_pdf, generated_data, sort_key="id")

    # Verify specific column types
    assert read_df.schema()["id"].dtype._is_integer_type()
    assert read_df.schema()["float_col"].dtype._is_floating_type()
    assert read_df.schema()["string_col"].dtype == daft.DataType.string()
    assert read_df.schema()["bool_col"].dtype == daft.DataType.bool()
    assert read_df.schema()["date_col"].dtype == daft.DataType.date()
    assert read_df.schema()["date_time_col"].dtype._is_temporal_type()


@pytest.mark.integration()
def test_write_sql_empty_dataframe(empty_test_db_for_write) -> None:
    """Test writing an empty DataFrame."""
    empty_data = pd.DataFrame(
        {
            "id": pd.Series(dtype="int64"),
            "name": pd.Series(dtype="str"),
        }
    )

    df = daft.from_pandas(empty_data)
    result_df = df.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite")
    result_df = result_df.collect()

    # Verify metadata shows 0 rows written
    result_dict = result_df.to_pydict()
    assert result_dict["rows_written"][0] == 0
    assert result_dict["table_name"][0] == WRITE_TEST_TABLE_NAME

    # Verify table exists and is empty
    read_df = daft.read_sql(
        f"SELECT * FROM {WRITE_TEST_TABLE_NAME}",
        empty_test_db_for_write,
        schema={"id": daft.DataType.int64(), "name": daft.DataType.string()},
    )
    read_pdf = read_df.to_pandas()
    assert len(read_pdf) == 0


@pytest.mark.integration()
def test_write_sql_with_nulls(empty_test_db_for_write) -> None:
    """Test that null values are handled correctly."""
    data_with_nulls = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "nullable_int": [1, None, 3, None, 5],
            "nullable_string": ["a", None, "c", None, "e"],
            "nullable_float": [1.1, 2.2, None, 4.4, None],
            "all_nulls": [None, None, None, None, None],
        }
    )

    df = daft.from_pandas(data_with_nulls)
    result_df = df.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite")
    result_df.collect()

    # Read back and verify nulls are preserved
    read_df = daft.read_sql(f"SELECT * FROM {WRITE_TEST_TABLE_NAME}", empty_test_db_for_write)
    read_pdf = read_df.to_pandas()

    assert len(read_pdf) == len(data_with_nulls)
    # Check nulls are in the right places
    assert read_pdf["nullable_int"].isna().sum() == 2
    assert read_pdf["nullable_string"].isna().sum() == 2
    assert read_pdf["nullable_float"].isna().sum() == 2
    assert read_pdf["all_nulls"].isna().sum() == 5


@pytest.mark.integration()
def test_write_sql_bad_url() -> None:
    """Test that invalid connection string raises an error."""
    df = daft.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    with pytest.raises(Exception):  # Could be RuntimeError or ValueError depending on implementation
        result_df = df.write_sql(WRITE_TEST_TABLE_NAME, "bad_url://invalid", mode="overwrite")
        result_df.collect()


@pytest.mark.integration()
def test_write_sql_connection_factory_ok(empty_test_db_for_write, generated_data) -> None:
    """Test that connection factory works correctly."""

    def create_conn():
        return sqlalchemy.create_engine(empty_test_db_for_write).connect()

    df = daft.from_pandas(generated_data)
    result_df = df.write_sql(WRITE_TEST_TABLE_NAME, create_conn, mode="overwrite")
    result_df = result_df.collect()

    # Verify metadata
    result_dict = result_df.to_pydict()
    assert result_dict["rows_written"][0] == len(generated_data)

    # Verify data was written correctly
    read_df = daft.read_sql(f"SELECT * FROM {WRITE_TEST_TABLE_NAME}", empty_test_db_for_write)
    assert_df_equals(read_df.to_pandas(coerce_temporal_nanoseconds=True), generated_data, sort_key="id")


@pytest.mark.integration()
def test_write_sql_bad_connection_factory() -> None:
    """Test that invalid connection factory raises an error."""
    df = daft.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    with pytest.raises(Exception):
        result_df = df.write_sql(WRITE_TEST_TABLE_NAME, lambda: "bad_conn", mode="overwrite")
        result_df.collect()


@pytest.mark.integration()
def test_write_sql_roundtrip_basic(empty_test_db_for_write, generated_data) -> None:
    """Test that data round-trips correctly through write_sql and read_sql."""
    # Write data
    write_df = daft.from_pandas(generated_data)
    result_df = write_df.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite")
    result_df.collect()

    # Read it back
    read_df = daft.read_sql(f"SELECT * FROM {WRITE_TEST_TABLE_NAME}", empty_test_db_for_write)
    read_pdf = read_df.to_pandas(coerce_temporal_nanoseconds=True)

    # Should be identical to original data
    assert_df_equals(read_pdf, generated_data, sort_key="id")


@pytest.mark.integration()
@pytest.mark.parametrize("num_partitions", [2, 4])
def test_write_sql_roundtrip_with_partitioned_read(empty_test_db_for_write, generated_data, num_partitions) -> None:
    """Test that data written with write_sql can be read with partitioned reads."""
    # Write data
    write_df = daft.from_pandas(generated_data)
    result_df = write_df.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite")
    result_df.collect()

    # Read it back with partitioning
    read_df = daft.read_sql(
        f"SELECT * FROM {WRITE_TEST_TABLE_NAME}",
        empty_test_db_for_write,
        partition_col="id",
        num_partitions=num_partitions,
    )

    assert read_df.num_partitions() == num_partitions
    read_pdf = read_df.to_pandas(coerce_temporal_nanoseconds=True)
    assert_df_equals(read_pdf, generated_data, sort_key="id")


@pytest.mark.integration()
def test_write_sql_roundtrip_all_data_types(empty_test_db_for_write) -> None:
    """Test round-trip with various data types."""
    test_data = pd.DataFrame(
        {
            "int_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "string_col": ["apple", "banana", "cherry", "date", "elderberry"],
            "bool_col": [True, False, True, False, True],
            "date_col": [
                datetime.date(2021, 1, 1),
                datetime.date(2021, 2, 1),
                datetime.date(2021, 3, 1),
                datetime.date(2021, 4, 1),
                datetime.date(2021, 5, 1),
            ],
            "datetime_col": [
                datetime.datetime(2020, 1, 1, 10, 0, 0),
                datetime.datetime(2020, 2, 1, 11, 0, 0),
                datetime.datetime(2020, 3, 1, 12, 0, 0),
                datetime.datetime(2020, 4, 1, 13, 0, 0),
                datetime.datetime(2020, 5, 1, 14, 0, 0),
            ],
        }
    )

    # Write
    write_df = daft.from_pandas(test_data)
    result_df = write_df.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite")
    result_df.collect()

    # Read back
    read_df = daft.read_sql(f"SELECT * FROM {WRITE_TEST_TABLE_NAME}", empty_test_db_for_write)
    read_pdf = read_df.to_pandas(coerce_temporal_nanoseconds=True)

    # Verify
    assert_df_equals(read_pdf, test_data, sort_key="int_col")


@pytest.mark.integration()
def test_write_sql_multiple_consecutive_writes(empty_test_db_for_write) -> None:
    """Test multiple consecutive writes with different modes."""
    # First write
    data1 = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    df1 = daft.from_pandas(data1)
    df1.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite").collect()

    # Verify first write
    read1 = daft.read_sql(f"SELECT * FROM {WRITE_TEST_TABLE_NAME}", empty_test_db_for_write)
    assert len(read1.to_pandas()) == 3

    # Append
    data2 = pd.DataFrame({"id": [4, 5], "value": [40, 50]})
    df2 = daft.from_pandas(data2)
    df2.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="append").collect()

    # Verify append
    read2 = daft.read_sql(f"SELECT * FROM {WRITE_TEST_TABLE_NAME}", empty_test_db_for_write)
    assert len(read2.to_pandas()) == 5

    # Overwrite
    data3 = pd.DataFrame({"id": [100], "value": [1000]})
    df3 = daft.from_pandas(data3)
    df3.write_sql(WRITE_TEST_TABLE_NAME, empty_test_db_for_write, mode="overwrite").collect()

    # Verify overwrite replaced everything
    read3 = daft.read_sql(f"SELECT * FROM {WRITE_TEST_TABLE_NAME}", empty_test_db_for_write)
    read3_pdf = read3.to_pandas()
    assert len(read3_pdf) == 1
    assert read3_pdf["id"].iloc[0] == 100
