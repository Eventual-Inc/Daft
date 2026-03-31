"""Tests for DataFrame.write_paimon() — writes to a local Paimon filesystem catalog.

All tests run without Docker or external services. Data written via Daft is
verified by reading back with both daft.read_paimon() and pypaimon's native
reader to ensure correctness.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")

import daft

# Import paimon_write to apply the patch for complex types support
from daft.io.paimon import paimon_write  # noqa: F401

# ---------------------------------------------------------------------------
# Basic append
# ---------------------------------------------------------------------------


def test_write_paimon_append_basic(append_only_table):
    """write_paimon with mode='append' should persist data readable by read_paimon."""
    table, _ = append_only_table
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "charlie"],
            "value": [1.1, 2.2, 3.3],
            "dt": ["2024-01-01", "2024-01-01", "2024-01-01"],
        }
    )
    df.write_paimon(table)

    result = daft.read_paimon(table).sort("id").to_arrow()
    assert result.num_rows == 3
    assert result.column("id").to_pylist() == [1, 2, 3]
    assert result.column("name").to_pylist() == ["alice", "bob", "charlie"]


def test_write_paimon_append_returns_summary(append_only_table):
    """write_paimon should return a DataFrame with operation metadata columns."""
    table, _ = append_only_table
    df = daft.from_pydict(
        {
            "id": [10, 20],
            "name": ["x", "y"],
            "value": [5.0, 6.0],
            "dt": ["2024-02-01", "2024-02-01"],
        }
    )
    result = df.write_paimon(table)
    result_dict = result.to_pydict()

    assert "operation" in result_dict
    assert "rows" in result_dict
    assert "file_size" in result_dict
    assert "file_name" in result_dict

    assert all(op == "ADD" for op in result_dict["operation"])
    assert sum(result_dict["rows"]) == 2
    assert all(s > 0 for s in result_dict["file_size"])
    assert all(len(fn) > 0 for fn in result_dict["file_name"])


def test_write_paimon_append_multiple_times(append_only_table):
    """Multiple append writes should accumulate rows."""
    table, _ = append_only_table
    df1 = daft.from_pydict({"id": [1], "name": ["a"], "value": [1.0], "dt": ["2024-01-01"]})
    df2 = daft.from_pydict({"id": [2], "name": ["b"], "value": [2.0], "dt": ["2024-01-02"]})
    df1.write_paimon(table)
    df2.write_paimon(table)

    result = daft.read_paimon(table).sort("id").to_arrow()
    assert result.num_rows == 2
    assert result.column("id").to_pylist() == [1, 2]


def test_write_paimon_roundtrip_native_verify(append_only_table):
    """Data written by Daft should also be readable via pypaimon's native reader."""
    table, _ = append_only_table
    df = daft.from_pydict(
        {
            "id": [7, 8, 9],
            "name": ["p", "q", "r"],
            "value": [7.0, 8.0, 9.0],
            "dt": ["2024-05-01", "2024-05-01", "2024-05-01"],
        }
    )
    df.write_paimon(table)

    # Verify via pypaimon native reader
    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    table_read = read_builder.new_read()
    splits = table_scan.plan().splits()
    arrow_table = table_read.to_arrow(splits)

    assert arrow_table.num_rows == 3
    ids = sorted(arrow_table.column("id").to_pylist())
    assert ids == [7, 8, 9]


# ---------------------------------------------------------------------------
# Overwrite
# ---------------------------------------------------------------------------


def test_write_paimon_overwrite_full(append_only_table):
    """mode='overwrite' should replace all existing data."""
    table, _ = append_only_table
    initial = daft.from_pydict(
        {"id": [1, 2], "name": ["a", "b"], "value": [1.0, 2.0], "dt": ["2024-01-01", "2024-01-01"]}
    )
    initial.write_paimon(table)

    replacement = daft.from_pydict({"id": [100], "name": ["z"], "value": [99.0], "dt": ["2024-06-01"]})
    result = replacement.write_paimon(table, mode="overwrite")
    result_dict = result.to_pydict()
    assert all(op == "OVERWRITE" for op in result_dict["operation"])

    final = daft.read_paimon(table).to_arrow()
    assert final.num_rows == 1
    assert final.column("id").to_pylist() == [100]


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_write_paimon_invalid_mode(append_only_table):
    """An unsupported mode should raise a ValueError."""
    table, _ = append_only_table
    df = daft.from_pydict({"id": [1], "name": ["a"], "value": [1.0], "dt": ["2024-01-01"]})
    with pytest.raises(ValueError, match="Only 'append' or 'overwrite' mode is supported"):
        df.write_paimon(table, mode="upsert")


def test_write_paimon_pk_table(pk_table):
    """Writing to a PK table should work and be readable back via Daft."""
    table, _ = pk_table
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["x", "y", "z"],
            "dt": ["2024-01-01", "2024-01-01", "2024-01-01"],
        }
    )
    df.write_paimon(table)

    result = daft.read_paimon(table).sort("id").to_arrow()
    assert result.num_rows == 3
    assert result.column("id").to_pylist() == [1, 2, 3]


# ---------------------------------------------------------------------------
# Schema conversion tests
# ---------------------------------------------------------------------------


class TestSchemaConversion:
    """Tests for schema conversion utilities."""

    def test_write_large_string_conversion(self, local_paimon_catalog):
        """Test that large_string columns are converted to string for pypaimon.

        Daft uses large_string internally, but pypaimon doesn't support it.
        The write path should automatically convert.
        """
        catalog, tmp_path = local_paimon_catalog

        # Create table with string column
        pa_schema = pa.schema([("id", pa.int64()), ("text", pa.string())])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.large_str", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.large_str")

        # Create DataFrame with large_string (Daft's default)
        df = daft.from_pydict({"id": [1, 2, 3], "text": ["a", "b", "c"]})

        # Write should succeed (conversion happens internally)
        df.write_paimon(table, mode="append")

        # Verify data
        result = daft.read_paimon(table).to_pydict()
        assert result["id"] == [1, 2, 3]
        assert result["text"] == ["a", "b", "c"]

    def test_write_large_binary_conversion(self, local_paimon_catalog):
        """Test that large_binary columns are converted to binary for pypaimon."""
        catalog, tmp_path = local_paimon_catalog

        # Create table with binary column
        pa_schema = pa.schema([("id", pa.int64()), ("data", pa.binary())])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.large_bin", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.large_bin")

        # Create DataFrame with binary data
        df = daft.from_pydict({"id": [1, 2], "data": [b"abc", b"def"]})

        # Write should succeed (conversion happens internally)
        df.write_paimon(table, mode="append")

        # Verify data
        result = daft.read_paimon(table).to_pydict()
        assert result["id"] == [1, 2]


# ---------------------------------------------------------------------------
# Complex type tests
# ---------------------------------------------------------------------------


class TestComplexTypes:
    """Tests for writing complex data types."""

    def test_write_nested_list(self, local_paimon_catalog):
        """Test writing Paimon table with list type.

        Note: Writing complex types requires a workaround because pypaimon's
        stats computation uses pyarrow min/max which doesn't support these types.
        Daft patches pypaimon's _get_column_stats to handle this gracefully.
        """
        catalog, tmp_path = local_paimon_catalog

        pa_schema = pa.schema([
            ("id", pa.int64()),
            ("list_col", pa.list_(pa.int64())),
        ])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.write_list", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.write_list")

        # Create DataFrame with list data
        df = daft.from_pydict({
            "id": [1, 2],
            "list_col": [[1, 2, 3], [4, 5]],
        })

        # Write should succeed with the patch
        df.write_paimon(table, mode="append")

        # Verify data
        result = daft.read_paimon(table).to_pydict()
        assert result["id"] == [1, 2]
