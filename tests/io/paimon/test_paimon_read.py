"""Tests for daft.read_paimon() — reads from a local Paimon filesystem catalog.

All tests run without Docker or external services; pypaimon is used directly
to create and populate test tables, then Daft's read_paimon() is validated.
"""

from __future__ import annotations

import decimal

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


# ---------------------------------------------------------------------------
# Filter pushdown tests
# ---------------------------------------------------------------------------


class TestFilterPushdown:
    """Tests for filter pushdown to Paimon."""

    @pytest.fixture
    def filter_table(self, local_paimon_catalog):
        """Create a table for filter pushdown tests."""
        catalog, tmp_path = local_paimon_catalog
        pa_schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.filter_test", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.filter_test")

        data = pa.table({"id": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "d", "e"]})
        _write_to_paimon(table, data)
        return table

    def test_filter_pushdown_equal(self, filter_table):
        """Test filter pushdown for equality comparison."""
        df = daft.read_paimon(filter_table).where(daft.col("id") == 3)
        result = df.to_pydict()

        assert result["id"] == [3]
        assert result["value"] == ["c"]

    def test_filter_pushdown_comparison(self, filter_table):
        """Test filter pushdown for comparison operators."""
        # Test greater than
        df = daft.read_paimon(filter_table).where(daft.col("id") > 3)
        result = df.to_pydict()
        assert result["id"] == [4, 5]

        # Test less than or equal
        df = daft.read_paimon(filter_table).where(daft.col("id") <= 2)
        result = df.to_pydict()
        assert result["id"] == [1, 2]

    def test_filter_pushdown_is_in(self, filter_table):
        """Test filter pushdown for IS IN operator."""
        df = daft.read_paimon(filter_table).where(daft.col("id").is_in([2, 4]))
        result = df.to_pydict()
        assert result["id"] == [2, 4]

    def test_filter_pushdown_is_null(self, local_paimon_catalog):
        """Test filter pushdown for IS NULL operator."""
        catalog, tmp_path = local_paimon_catalog

        pa_schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.filter_null", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.filter_null")

        # Write data with nulls
        data = pa.table({"id": [1, 2, 3], "value": ["a", None, "c"]})
        _write_to_paimon(table, data)

        # Read with IS NULL filter
        df = daft.read_paimon(table).where(daft.col("value").is_null())
        result = df.to_pydict()
        assert result["id"] == [2]

    def test_filter_pushdown_string(self, filter_table):
        """Test filter pushdown for string operations."""
        # Test starts_with
        df = daft.read_paimon(filter_table).where(daft.col("value").startswith("a"))
        result = df.to_pydict()
        assert result["value"] == ["a"]

        # Test contains
        df = daft.read_paimon(filter_table).where(daft.col("value").contains("b"))
        result = df.to_pydict()
        assert result["value"] == ["b"]

    def test_filter_pushdown_combined(self, filter_table):
        """Test combined filter pushdown with AND."""
        df = daft.read_paimon(filter_table).where((daft.col("id") >= 2) & (daft.col("id") <= 4))
        result = df.to_pydict()
        assert result["id"] == [2, 3, 4]


# ---------------------------------------------------------------------------
# Advanced data types
# ---------------------------------------------------------------------------


class TestNestedTypes:
    """Tests for nested data types (list, map, struct)."""

    def test_read_nested_list(self, local_paimon_catalog):
        """Test reading Paimon table with list type.

        Note: Writing complex types requires a workaround because pypaimon's
        stats computation uses pyarrow min/max which doesn't support these types.
        Daft patches pypaimon's _get_column_stats to handle this gracefully.
        """
        catalog, tmp_path = local_paimon_catalog

        pa_schema = pa.schema(
            [
                ("id", pa.int64()),
                ("list_col", pa.list_(pa.int64())),
            ]
        )
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.nested_list", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.nested_list")

        data = pa.table(
            {
                "id": [1, 2],
                "list_col": [[1, 2, 3], [4, 5]],
            },
            schema=pa_schema,
        )
        _write_to_paimon(table, data)

        df = daft.read_paimon(table)
        result = df.to_pydict()

        assert len(result["id"]) == 2
        assert result["list_col"][0] == [1, 2, 3]

    def test_read_nested_map(self, local_paimon_catalog):
        """Test reading Paimon table with map type."""
        catalog, tmp_path = local_paimon_catalog

        pa_schema = pa.schema(
            [
                ("id", pa.int64()),
                ("map_col", pa.map_(pa.string(), pa.int64())),
            ]
        )
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.nested_map", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.nested_map")

        data = pa.table(
            {
                "id": [1, 2],
                "map_col": [[("k1", 1), ("k2", 2)], [("k3", 3)]],
            },
            schema=pa_schema,
        )
        _write_to_paimon(table, data)

        df = daft.read_paimon(table)
        result = df.to_pydict()

        assert len(result["id"]) == 2


class TestDecimalType:
    """Tests for decimal type support."""

    def test_read_decimal(self, local_paimon_catalog):
        """Test reading Paimon table with decimal type."""
        catalog, tmp_path = local_paimon_catalog

        pa_schema = pa.schema(
            [
                ("id", pa.int64()),
                ("amount", pa.decimal128(10, 2)),
            ]
        )
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.decimal_table", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.decimal_table")

        # Write data using decimal.Decimal
        data = pa.table(
            {
                "id": [1, 2, 3],
                "amount": [decimal.Decimal("100.50"), decimal.Decimal("200.75"), decimal.Decimal("300.00")],
            },
            schema=pa_schema,
        )
        _write_to_paimon(table, data)

        df = daft.read_paimon(table)
        result = df.to_pydict()

        assert len(result["id"]) == 3


class TestNullValues:
    """Tests for null value handling."""

    def test_read_with_nulls(self, local_paimon_catalog):
        """Test reading table with null values."""
        catalog, tmp_path = local_paimon_catalog

        pa_schema = pa.schema(
            [
                ("id", pa.int64()),
                ("name", pa.string()),
                ("value", pa.float64()),
            ]
        )
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("test_db.null_table", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.null_table")

        # Write data with nulls
        data = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["alice", None, "charlie"],
                "value": [1.1, 2.2, None],
            }
        )
        _write_to_paimon(table, data)

        df = daft.read_paimon(table)
        result = df.to_pydict()

        assert len(result["id"]) == 3
        assert result["name"][1] is None
        assert result["value"][2] is None


class TestCompositePrimaryKey:
    """Tests for composite primary key tables."""

    def test_read_composite_pk_table(self, local_paimon_catalog):
        """Test reading from a table with composite primary key."""
        catalog, tmp_path = local_paimon_catalog

        pa_schema = pa.schema(
            [
                ("pk1", pa.int64()),
                ("pk2", pa.string()),
                ("value", pa.float64()),
            ]
        )
        # Composite primary key table
        paimon_schema = pypaimon.Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=["pk1", "pk2"],
            options={"bucket": "2"},
        )
        catalog.create_table("test_db.composite_pk", paimon_schema, ignore_if_exists=True)
        table = catalog.get_table("test_db.composite_pk")

        # Write data
        data = pa.table(
            {
                "pk1": [1, 1, 2],
                "pk2": ["a", "b", "a"],
                "value": [1.1, 2.2, 3.3],
            }
        )
        _write_to_paimon(table, data)

        df = daft.read_paimon(table)
        result = df.to_pydict()

        assert len(result["pk1"]) == 3
