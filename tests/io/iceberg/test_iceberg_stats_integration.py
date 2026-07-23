"""Integration tests for Iceberg scan task statistics injection."""

from __future__ import annotations

from unittest.mock import patch

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
)

import daft
from daft.daft import PyPushdowns, StorageConfig
from daft.io.iceberg.iceberg_scan import IcebergScanOperator


@pytest.fixture(scope="function")
def local_catalog(tmpdir):
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
        warehouse=f"file://{tmpdir}",
    )
    catalog.create_namespace("default")
    yield catalog
    catalog.engine.dispose()


@pytest.fixture(scope="function")
def multi_file_table(local_catalog, tmpdir):
    """Create a table with multiple data files for stats testing."""
    schema = IcebergSchema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="value", field_type=DoubleType(), required=False),
    )

    table = local_catalog.create_table(
        identifier="default.test_stats",
        schema=schema,
    )

    # Write multiple files with different data
    # File 1: ids 1-100
    data1 = pa.table(
        {
            "id": pa.array(range(1, 101), type=pa.int64()),
            "name": pa.array([f"row_{i}" for i in range(1, 101)]),
            "value": pa.array([float(i) * 1.5 for i in range(1, 101)]),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.string(), nullable=True),
                pa.field("value", pa.float64(), nullable=True),
            ]
        ),
    )
    table.append(data1)

    # File 2: ids 101-200
    data2 = pa.table(
        {
            "id": pa.array(range(101, 201), type=pa.int64()),
            "name": pa.array([f"row_{i}" for i in range(101, 201)]),
            "value": pa.array([float(i) * 1.5 for i in range(101, 201)]),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.string(), nullable=True),
                pa.field("value", pa.float64(), nullable=True),
            ]
        ),
    )
    table.append(data2)

    # File 3: ids 201-300
    data3 = pa.table(
        {
            "id": pa.array(range(201, 301), type=pa.int64()),
            "name": pa.array([f"row_{i}" for i in range(201, 301)]),
            "value": pa.array([float(i) * 1.5 for i in range(201, 301)]),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.string(), nullable=True),
                pa.field("value", pa.float64(), nullable=True),
            ]
        ),
    )
    table.append(data3)

    return table


class TestIcebergStatsIntegration:
    """Integration tests for Iceberg stats injection."""

    def test_scan_tasks_have_stats_injected(self, multi_file_table):
        """Verify each ScanTask receives stats with expected columns."""
        storage_config = StorageConfig(multithreaded_io=False, io_config=daft.io.IOConfig())
        operator = IcebergScanOperator(multi_file_table, None, storage_config)

        pushdowns = PyPushdowns()

        # Collect all stats passed to catalog_scan_task
        captured_stats = []
        original_catalog_scan_task = daft.daft.ScanTask.catalog_scan_task

        def capturing_catalog_scan_task(**kwargs):
            captured_stats.append(kwargs.get("stats"))
            return original_catalog_scan_task(**kwargs)

        with patch.object(daft.daft.ScanTask, "catalog_scan_task", side_effect=capturing_catalog_scan_task):
            scan_tasks = list(operator.to_scan_tasks(pushdowns))

        assert len(scan_tasks) == 3

        # Each scan task should have received non-None stats
        for i, stats_rb in enumerate(captured_stats):
            assert stats_rb is not None, f"ScanTask {i} received stats=None"

        # Stats should contain the primitive columns (id, value) but not nested
        # Verify by checking the first task's stats schema
        first_stats = captured_stats[0]
        arrow_rb = first_stats.to_arrow_record_batch()
        col_names = set(arrow_rb.schema.names)
        assert "id" in col_names, f"Expected 'id' in stats columns, got {col_names}"
        assert "value" in col_names, f"Expected 'value' in stats columns, got {col_names}"

    def test_scan_task_stats_values_are_correct(self, multi_file_table):
        """Verify stats values match the actual data ranges."""
        storage_config = StorageConfig(multithreaded_io=False, io_config=daft.io.IOConfig())
        operator = IcebergScanOperator(multi_file_table, None, storage_config)

        pushdowns = PyPushdowns()

        captured_stats = []
        original_catalog_scan_task = daft.daft.ScanTask.catalog_scan_task

        def capturing_catalog_scan_task(**kwargs):
            captured_stats.append(kwargs.get("stats"))
            return original_catalog_scan_task(**kwargs)

        with patch.object(daft.daft.ScanTask, "catalog_scan_task", side_effect=capturing_catalog_scan_task):
            list(operator.to_scan_tasks(pushdowns))

        # Collect all id min/max pairs (order may vary)
        id_ranges = []
        for stats_rb in captured_stats:
            arrow_rb = stats_rb.to_arrow_record_batch()
            id_min = arrow_rb.column("id")[0].as_py()
            id_max = arrow_rb.column("id")[1].as_py()
            id_ranges.append((id_min, id_max))

        # Sort by min value for deterministic comparison
        id_ranges.sort()

        # File 1: ids 1-100
        assert id_ranges[0] == (1, 100)
        # File 2: ids 101-200
        assert id_ranges[1] == (101, 200)
        # File 3: ids 201-300
        assert id_ranges[2] == (201, 300)

    def test_query_results_unchanged(self, multi_file_table):
        """Verify query results are correct with stats injection."""
        df = daft.read_iceberg(multi_file_table)
        result = df.to_pydict()

        assert len(result["id"]) == 300
        assert sorted(result["id"]) == list(range(1, 301))

    def test_query_with_filter(self, multi_file_table):
        """Verify filtered query results are correct with stats injection."""
        df = daft.read_iceberg(multi_file_table)
        filtered = df.where(daft.col("id") > 200)
        result = filtered.to_pydict()

        assert len(result["id"]) == 100
        assert sorted(result["id"]) == list(range(201, 301))

    def test_query_with_complex_filter(self, multi_file_table):
        """Verify complex filtered query results are correct."""
        df = daft.read_iceberg(multi_file_table)
        filtered = df.where((daft.col("id") >= 50) & (daft.col("id") <= 150))
        result = filtered.to_pydict()

        assert len(result["id"]) == 101
        assert sorted(result["id"]) == list(range(50, 151))

    def test_scan_tasks_with_partitioned_table(self, local_catalog, tmpdir):
        """Verify stats injection works with partitioned tables."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="category", field_type=StringType(), required=False),
            NestedField(field_id=3, name="value", field_type=DoubleType(), required=False),
        )

        from pyiceberg.partitioning import PartitionField, PartitionSpec

        table = local_catalog.create_table(
            identifier="default.test_stats_partitioned",
            schema=schema,
            partition_spec=PartitionSpec(
                PartitionField(
                    source_id=2,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="category",
                )
            ),
        )

        data1 = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "category": pa.array(["A", "A", "A"]),
                "value": pa.array([1.0, 2.0, 3.0]),
            },
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("category", pa.string(), nullable=True),
                    pa.field("value", pa.float64(), nullable=True),
                ]
            ),
        )
        table.append(data1)

        data2 = pa.table(
            {
                "id": pa.array([4, 5, 6], type=pa.int64()),
                "category": pa.array(["B", "B", "B"]),
                "value": pa.array([4.0, 5.0, 6.0]),
            },
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("category", pa.string(), nullable=True),
                    pa.field("value", pa.float64(), nullable=True),
                ]
            ),
        )
        table.append(data2)

        # Verify stats are injected for partitioned table too
        storage_config = StorageConfig(multithreaded_io=False, io_config=daft.io.IOConfig())
        operator = IcebergScanOperator(table, None, storage_config)
        pushdowns = PyPushdowns()

        captured_stats = []
        original_catalog_scan_task = daft.daft.ScanTask.catalog_scan_task

        def capturing_catalog_scan_task(**kwargs):
            captured_stats.append(kwargs.get("stats"))
            return original_catalog_scan_task(**kwargs)

        with patch.object(daft.daft.ScanTask, "catalog_scan_task", side_effect=capturing_catalog_scan_task):
            scan_tasks = list(operator.to_scan_tasks(pushdowns))

        assert len(scan_tasks) == 2
        for i, stats_rb in enumerate(captured_stats):
            assert stats_rb is not None, f"Partitioned scan task {i} should have stats"

    def test_scan_tasks_with_table_without_stats(self, local_catalog, tmpdir):
        """Verify degradation when PyIceberg DataFile has no bounds.

        Uses a mock DataFile with lower_bounds/upper_bounds=None to simulate
        old Iceberg metadata or stats-stripped files.
        """
        from unittest.mock import Mock

        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )

        table = local_catalog.create_table(
            identifier="default.test_no_stats",
            schema=schema,
        )

        data = pa.table(
            {"id": pa.array([1, 2, 3], type=pa.int64())},
            schema=pa.schema([pa.field("id", pa.int64(), nullable=False)]),
        )
        table.append(data)

        # Patch _build_iceberg_scan_task_stats to simulate missing bounds
        from daft.io.iceberg import iceberg_scan

        original_build = iceberg_scan._build_iceberg_scan_task_stats

        def mock_build_no_stats(*args, **kwargs):
            # Simulate DataFile with no bounds
            mock_file = Mock()
            mock_file.lower_bounds = None
            mock_file.upper_bounds = None
            return original_build(mock_file, args[1], args[2], args[3])

        storage_config = StorageConfig(multithreaded_io=False, io_config=daft.io.IOConfig())
        operator = IcebergScanOperator(table, None, storage_config)
        pushdowns = PyPushdowns()

        captured_stats = []
        original_catalog_scan_task = daft.daft.ScanTask.catalog_scan_task

        def capturing_catalog_scan_task(**kwargs):
            captured_stats.append(kwargs.get("stats"))
            return original_catalog_scan_task(**kwargs)

        with (
            patch.object(iceberg_scan, "_build_iceberg_scan_task_stats", side_effect=mock_build_no_stats),
            patch.object(daft.daft.ScanTask, "catalog_scan_task", side_effect=capturing_catalog_scan_task),
        ):
            scan_tasks = list(operator.to_scan_tasks(pushdowns))

            assert len(scan_tasks) == 1
            # Stats should be None when bounds are missing
            assert captured_stats[0] is None, "Expected stats=None when bounds are missing"

            # Query should still work correctly when stats are None
            df = daft.read_iceberg(table)
            result = df.to_pydict()
            assert len(result["id"]) == 3
