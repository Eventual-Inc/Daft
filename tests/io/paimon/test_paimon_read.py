"""Tests for daft.read_paimon() — reads from a local Paimon filesystem catalog.

All tests run without Docker or external services; pypaimon is used directly
to create and populate test tables, then Daft's read_paimon() is validated.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")

import daft
from tests.io.paimon.conftest import _write_to_paimon

# ---------------------------------------------------------------------------
# Basic read roundtrip
# ---------------------------------------------------------------------------


def test_read_paimon_basic(append_only_table):
    """Write data via pypaimon, read back via daft.read_paimon, verify correctness."""
    table, tmp_path = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "name": pa.array(["alice", "bob", "charlie"], pa.string()),
            "value": pa.array([1.1, 2.2, 3.3], pa.float64()),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = daft.read_paimon(table)
    result = df.sort("id").to_arrow()

    assert result.num_rows == 3
    assert result.schema.field("id").type == pa.int64()
    # Daft normalises string → large_string internally
    assert result.schema.field("name").type in (pa.string(), pa.large_string())
    assert result.column("id").to_pylist() == [1, 2, 3]
    assert result.column("name").to_pylist() == ["alice", "bob", "charlie"]


def test_read_paimon_empty_table(append_only_table):
    """Reading an empty table should return a DataFrame with the correct schema but zero rows."""
    table, _ = append_only_table
    df = daft.read_paimon(table)
    result = df.to_arrow()

    assert result.num_rows == 0
    assert "id" in result.schema.names
    assert "name" in result.schema.names
    assert "dt" in result.schema.names


def test_read_paimon_schema_matches(append_only_table):
    """The schema reported by read_paimon() should match the Paimon table schema."""
    table, _ = append_only_table
    df = daft.read_paimon(table)
    schema = df.schema()

    assert "id" in schema.column_names()
    assert "name" in schema.column_names()
    assert "value" in schema.column_names()
    assert "dt" in schema.column_names()


# ---------------------------------------------------------------------------
# Multi-partition reads
# ---------------------------------------------------------------------------


def test_read_paimon_multiple_partitions(append_only_table):
    """Data spread across multiple partitions should all be read back."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int64()),
            "name": pa.array(["a", "b", "c", "d"], pa.string()),
            "value": pa.array([1.0, 2.0, 3.0, 4.0], pa.float64()),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = daft.read_paimon(table)
    result = df.sort("id").to_arrow()

    assert result.num_rows == 4
    assert sorted(result.column("id").to_pylist()) == [1, 2, 3, 4]
    assert set(result.column("dt").to_pylist()) == {"2024-01-01", "2024-01-02"}


def test_read_paimon_partition_column_present(append_only_table):
    """Partition columns must appear in the result schema and have correct values."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([10], pa.int64()),
            "name": pa.array(["x"], pa.string()),
            "value": pa.array([9.9], pa.float64()),
            "dt": pa.array(["2024-03-15"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = daft.read_paimon(table)
    result = df.to_arrow()

    assert "dt" in result.schema.names
    assert result.column("dt").to_pylist() == ["2024-03-15"]


# ---------------------------------------------------------------------------
# Column projection
# ---------------------------------------------------------------------------


def test_read_paimon_column_projection(append_only_table):
    """Requesting a subset of columns should work without errors."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "name": pa.array(["a", "b"], pa.string()),
            "value": pa.array([1.0, 2.0], pa.float64()),
            "dt": pa.array(["2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = daft.read_paimon(table).select("id", "name")
    result = df.sort("id").to_arrow()

    assert result.schema.names == ["id", "name"]
    assert result.num_rows == 2
    assert result.column("id").to_pylist() == [1, 2]


def test_read_paimon_multiple_batches(local_paimon_catalog):
    """Writing data in two separate batches (two snapshots) should read all rows."""
    catalog, tmp_path = local_paimon_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("v", pa.string()),
                pa.field("dt", pa.string()),
            ]
        ),
        partition_keys=["dt"],
        options={"bucket": "1", "file.format": "parquet"},
    )
    catalog.create_table("test_db.multi_batch", schema, ignore_if_exists=False)
    table = catalog.get_table("test_db.multi_batch")

    batch1 = pa.table({"id": [1, 2], "v": ["a", "b"], "dt": ["2024-01-01", "2024-01-01"]})
    batch2 = pa.table({"id": [3, 4], "v": ["c", "d"], "dt": ["2024-01-02", "2024-01-02"]})

    _write_to_paimon(table, batch1)
    _write_to_paimon(table, batch2)

    df = daft.read_paimon(table)
    result = df.sort("id").to_arrow()

    assert result.num_rows == 4
    assert result.column("id").to_pylist() == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Primary-key table (LSM merge fall-back)
# ---------------------------------------------------------------------------


def test_read_paimon_pk_table_basic(pk_table):
    """Primary-key table: falls back to pypaimon reader, data should be correct."""
    table, _ = pk_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "name": pa.array(["x", "y", "z"], pa.string()),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    df = daft.read_paimon(table)
    result = df.sort("id").to_arrow()

    assert result.num_rows == 3
    assert result.column("id").to_pylist() == [1, 2, 3]
    assert result.column("name").to_pylist() == ["x", "y", "z"]


def test_read_paimon_pk_table_deduplication(pk_table):
    """Writing the same PK twice should result in the latest value being visible."""
    table, _ = pk_table
    batch1 = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "name": pa.array(["old_a", "old_b"], pa.string()),
            "dt": pa.array(["2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, batch1)

    batch2 = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "name": pa.array(["new_a"], pa.string()),
            "dt": pa.array(["2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, batch2)

    df = daft.read_paimon(table)
    result = df.sort("id").to_arrow()

    # After merge, id=1 should show the latest value
    assert result.num_rows == 2
    id1_row = [row for row in zip(result.column("id").to_pylist(), result.column("name").to_pylist()) if row[0] == 1]
    assert len(id1_row) == 1
    assert id1_row[0][1] == "new_a"


# ---------------------------------------------------------------------------
# Filter pushdown
# ---------------------------------------------------------------------------


def test_read_paimon_partition_filter(append_only_table):
    """Partition filter should prune partitions at scan time."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int64()),
            "name": pa.array(["a", "b", "c", "d"], pa.string()),
            "value": pa.array([1.0, 2.0, 3.0, 4.0], pa.float64()),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    # Filter on partition column
    df = daft.read_paimon(table).where(daft.col("dt") == "2024-01-01")
    result = df.sort("id").to_arrow()

    assert result.num_rows == 2
    assert result.column("id").to_pylist() == [1, 2]
    assert all(dt == "2024-01-01" for dt in result.column("dt").to_pylist())


def test_read_paimon_row_filter(append_only_table):
    """Row-level filter should be applied after reading data."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int64()),
            "name": pa.array(["alice", "bob", "charlie", "dave"], pa.string()),
            "value": pa.array([10.0, 20.0, 30.0, 40.0], pa.float64()),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01"], pa.string()),
        }
    )
    _write_to_paimon(table, data)

    # Filter on non-partition column
    df = daft.read_paimon(table).where(daft.col("id") > 2)
    result = df.sort("id").to_arrow()

    assert result.num_rows == 2
    assert result.column("id").to_pylist() == [3, 4]


def test_read_paimon_combined_filter(append_only_table):
    """Combined partition + row filter should work together."""
    table, _ = append_only_table
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
            "name": pa.array(["a", "b", "c", "d", "e", "f"], pa.string()),
            "value": pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], pa.float64()),
            "dt": pa.array(
                ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
                pa.string(),
            ),
        }
    )
    _write_to_paimon(table, data)

    # Filter on both partition and non-partition columns
    df = daft.read_paimon(table).where((daft.col("dt") == "2024-01-01") & (daft.col("id") == 2))
    result = df.to_arrow()

    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [2]
    assert result.column("dt").to_pylist() == ["2024-01-01"]
