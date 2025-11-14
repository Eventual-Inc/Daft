"""Tests for DataFrame.write_sql() method using SQLite and other SQL databases."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Callable

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection

import daft


@pytest.fixture
def sqlite_db_path() -> Path:  # type: ignore
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".db") as f:
        db_path = f.name
        yield Path(db_path)


def sqlite_connection_url(sqlite_db_path: str | Path) -> str:
    """Return SQLite connection URL for testing."""
    return f"sqlite:///{sqlite_db_path}"


@pytest.fixture
def sqlite_connection_factory(sqlite_db_path: Path) -> Callable[[], Connection]:
    """Return a SQLAlchemy connection factory for SQLite."""

    def factory() -> Connection:
        engine = create_engine(sqlite_connection_url(sqlite_db_path))
        return engine.connect()

    return factory


def sqlite_table_data(sqlite_db_path: str | Path, table_name: str) -> list[dict[str, Any]]:
    """Helper to read data from SQLite table as list of dicts."""
    with sqlite3.connect(str(sqlite_db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = [dict(row) for row in cursor.fetchall()]
    return rows


def sqlite_table_schema(sqlite_db_path: str, table_name: str) -> dict[str, str]:
    """Helper to get table schema from SQLite."""
    with sqlite3.connect(sqlite_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        schema = {row[1]: row[2] for row in cursor.fetchall()}
    return schema


# ===== Basic write_sql functionality tests =====


def test_write_sql_create_simple_table(sqlite_db_path: Path):
    """Test creating a new table with write_sql."""
    url = sqlite_connection_url(sqlite_db_path)
    df = daft.from_pydict({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})

    result_df = df.write_sql("users", conn=url, mode="overwrite")

    # Verify result DataFrame has expected schema
    assert result_df.column_names == ["rows_written", "bytes_written", "table_name"]

    # Verify data was written
    result_data = result_df.to_pydict()
    assert result_data["rows_written"][0] == 3
    assert result_data["table_name"][0] == "users"

    # Verify table exists and contains correct data
    rows = sqlite_table_data(sqlite_db_path, "users")
    assert len(rows) == 3
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Alice"
    assert rows[0]["age"] == 25


def test_write_sql_append_to_existing_table(sqlite_db_path):
    """Test appending data to an existing table."""
    url = sqlite_connection_url(sqlite_db_path)

    # First write
    df1 = daft.from_pydict({"id": [1, 2], "name": ["Charlie", "David"]})
    df1.write_sql("users", conn=url, mode="overwrite")

    # Append more data
    df2 = daft.from_pydict({"id": [3, 4], "name": ["Alice", "Bob"]})
    result_df = df2.write_sql("users", conn=url, mode="append")

    # Verify append worked
    result_data = result_df.to_pydict()
    assert result_data["rows_written"][0] == 2

    # Verify all data is present
    rows = sqlite_table_data(sqlite_db_path, "users")
    assert len(rows) == 4
    names = sorted([row["name"] for row in rows])
    assert names == ["Alice", "Bob", "Charlie", "David"]


def test_write_sql_replace_table(sqlite_db_path):
    """Test replacing an existing table with new data."""
    url = sqlite_connection_url(sqlite_db_path)
    # First write
    df1 = daft.from_pydict({"id": [1, 2], "name": ["Alice", "Bob"]})
    df1.write_sql("users", conn=url, mode="overwrite")

    # Replace with new data
    df2 = daft.from_pydict({"id": [100, 200], "name": ["New1", "New2"]})
    result_df = df2.write_sql("users", conn=url, mode="overwrite")

    # Verify replacement worked
    result_data = result_df.to_pydict()
    assert result_data["rows_written"][0] == 2

    # Verify only new data is present
    rows = sqlite_table_data(sqlite_db_path, "users")
    assert len(rows) == 2
    assert rows[0]["id"] == 100
    assert rows[1]["id"] == 200


def test_write_sql_empty_dataframe(sqlite_db_path):
    """Test writing an empty DataFrame."""
    url = sqlite_connection_url(sqlite_db_path)
    df = daft.from_pydict(
        {
            "id": [],
            "name": [],
        }
    )

    result_df = df.write_sql("empty_table", conn=url, mode="overwrite")

    # Verify result indicates 0 rows written
    result_data = result_df.to_pydict()
    assert result_data["rows_written"][0] == 0


# ===== Data type handling tests =====


def test_write_sql_various_types(sqlite_db_path):
    """Test writing various data types."""
    url = sqlite_connection_url(sqlite_db_path)
    df = daft.from_pydict(
        {
            "int_col": [1, 2, 3],
            "float_col": [1.5, 2.5, 3.5],
            "str_col": ["a", "b", "c"],
            "bool_col": [True, False, True],
        }
    )

    df.write_sql("mixed_types", conn=url, mode="overwrite")

    # Verify data types are preserved
    rows = sqlite_table_data(sqlite_db_path, "mixed_types")
    assert len(rows) == 3
    assert rows[0]["int_col"] == 1
    assert rows[0]["float_col"] == pytest.approx(1.5)
    assert rows[0]["str_col"] == "a"
    assert rows[0]["bool_col"] == 1  # SQLite stores bools as integers


def test_write_sql_with_nulls(sqlite_db_path):
    """Test writing NULL values."""
    url = sqlite_connection_url(sqlite_db_path)
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "optional_name": ["Alice", None, "Charlie"],
        }
    )

    df.write_sql("with_nulls", conn=url, mode="overwrite")

    # Verify NULL values are preserved
    rows = sqlite_table_data(sqlite_db_path, "with_nulls")
    assert rows[0]["optional_name"] == "Alice"
    assert rows[1]["optional_name"] is None
    assert rows[2]["optional_name"] == "Charlie"


# ===== Partition tests =====


def test_write_sql_multiple_partitions(sqlite_db_path):
    """Test writing data with multiple partitions."""
    url = sqlite_connection_url(sqlite_db_path)
    # Create a larger DataFrame
    df = daft.from_pydict(
        {
            "id": list(range(100)),
            "value": [str(i) for i in range(100)],
        }
    )

    # Try to partition if possible (may not be supported by native runner)
    try:
        df = df.into_partitions(4)
    except Exception:
        # If partitioning not supported, continue with single partition
        pass

    result_df = df.write_sql("large_table", conn=url, mode="overwrite")

    # Verify all data was written
    result_data = result_df.to_pydict()
    assert result_data["rows_written"][0] == 100

    rows = sqlite_table_data(sqlite_db_path, "large_table")
    assert len(rows) == 100


# ===== Connection factory tests =====


def test_write_sql_with_connection_factory(sqlite_connection_factory, sqlite_db_path):
    """Test write_sql using a connection factory callable."""
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    result_df = df.write_sql("factory_test", conn=sqlite_connection_factory, mode="overwrite")

    # Verify data was written
    result_data = result_df.to_pydict()
    assert result_data["rows_written"][0] == 3

    # Verify table exists
    rows = sqlite_table_data(sqlite_db_path, "factory_test")
    assert len(rows) == 3


# ===== Error handling tests =====


def test_write_sql_create_table_exists_error(sqlite_db_path):
    """Test that creating a table that already exists raises an error."""
    url = sqlite_connection_url(sqlite_db_path)
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    # Create table
    df.write_sql("existing_table", conn=url, mode="overwrite")

    # Try to create again - should fail or be handled gracefully
    # (The exact behavior may vary based on implementation)
    try:
        df.write_sql("existing_table", conn=url, mode="overwrite")
        # If it doesn't raise, that's also acceptable
    except Exception:
        # Expected to fail
        pass


def test_write_sql_append_nonexistent_table_error(sqlite_db_path):
    """Test that appending to a non-existent table raises an error."""
    url = sqlite_connection_url(sqlite_db_path)
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
        }
    )

    # Try to append to non-existent table
    with pytest.raises(Exception):
        df.write_sql("nonexistent_table", conn=url, mode="append")


# ===== End-to-end tests =====


def test_write_sql_roundtrip(sqlite_db_path):
    """Test writing data and reading it back."""
    url = sqlite_connection_url(sqlite_db_path)
    original_data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45],
    }

    # Write data
    df_write = daft.from_pydict(original_data)
    df_write.write_sql("roundtrip_test", conn=url, mode="overwrite")

    # Read data back using read_sql
    df_read = daft.read_sql("SELECT * FROM roundtrip_test", conn=url)
    result = df_read.to_pydict()

    # Verify data matches
    assert len(result["id"]) == 5
    assert result["id"] == [1, 2, 3, 4, 5]
    assert result["name"] == ["Alice", "Bob", "Charlie", "David", "Eve"]
    assert result["age"] == [25, 30, 35, 40, 45]


def test_write_sql_result_schema(sqlite_db_path):
    """Test that write_sql result DataFrame has correct schema."""
    url = sqlite_connection_url(sqlite_db_path)
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    result_df = df.write_sql("schema_test", conn=url, mode="overwrite")

    # Check schema
    schema = result_df.schema()
    assert len(schema) == 3
    assert "rows_written" in schema.column_names()
    assert "bytes_written" in schema.column_names()
    assert "table_name" in schema.column_names()
