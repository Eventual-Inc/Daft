"""Tests for Iceberg planning-phase scan instrumentation.

Verifies that get_iceberg_scan_metrics() / reset_iceberg_scan_metrics() correctly
capture files_listed_by_catalog, tasks_after_pruning, planning_time_ms,
bytes_scheduled, and count_pushdown_used after each scan planning call.
"""
from __future__ import annotations

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from daft.io.iceberg import get_iceberg_scan_metrics, reset_iceberg_scan_metrics
from daft.io.iceberg import iceberg_scan as _mod
from daft.io.iceberg.iceberg_scan import IcebergScanOperator
from daft.logical.schema import Schema


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _make_mock_table(record_counts: list[int], file_sizes: list[int] | None = None):
    """Minimal mock pyiceberg Table with configurable per-file record counts."""
    if file_sizes is None:
        file_sizes = [1024 * (i + 1) for i in range(len(record_counts))]

    mock_table = Mock()
    mock_table.name.return_value = ["test", "table"]

    mock_schema = Mock()
    mock_table.schema.return_value = mock_schema

    mock_spec = Mock()
    mock_spec.fields = []
    mock_table.spec.return_value = mock_spec
    mock_table.specs.return_value = {0: mock_spec}

    mock_scan = Mock()
    mock_table.scan.return_value = mock_scan
    mock_scan.projection.return_value = mock_schema

    tasks = []
    for i, (count, size) in enumerate(zip(record_counts, file_sizes)):
        task = Mock()
        f = Mock()
        f.record_count = count
        f.file_path = f"/fake/path_{i}.parquet"
        f.file_format = "PARQUET"
        f.file_size_in_bytes = size
        f.spec_id = 0
        f.partition = []
        task.file = f
        task.delete_files = []
        tasks.append(task)

    mock_scan.plan_files.return_value = tasks
    return mock_table


def _make_operator(mock_table):
    arrow_schema = pa.schema([pa.field("id", pa.int64())])
    with (
        patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow", return_value=arrow_schema),
        patch("daft.io.iceberg.iceberg_scan.visit", return_value={}),
        patch(
            "daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema",
            return_value=Schema.from_pyarrow_schema(arrow_schema),
        ),
    ):
        return IcebergScanOperator(mock_table, None, Mock())


def _make_pushdowns(limit=None):
    p = Mock()
    p.limit = limit
    p.filters = None
    p.partition_filters = None
    p.aggregation = None
    return p


def _run_regular_scan(op, pushdowns=None):
    """Drive _create_regular_scan_tasks to completion, suppressing ScanTask construction."""
    with patch("daft.io.iceberg.iceberg_scan.ScanTask.catalog_scan_task", return_value=Mock()):
        with patch.object(op, "_iceberg_record_to_partition_spec", return_value=None):
            return list(op._create_regular_scan_tasks(pushdowns or _make_pushdowns()))


# ---------------------------------------------------------------------------
# Planning-phase metric tests
# ---------------------------------------------------------------------------

class TestPlanningMetrics:
    def setup_method(self):
        reset_iceberg_scan_metrics()

    def test_files_listed_by_catalog_equals_plan_files_output(self):
        """files_listed_by_catalog reflects the raw plan_files() count before any filtering."""
        op = _make_operator(_make_mock_table([100, 200, 300]))
        _run_regular_scan(op)

        assert get_iceberg_scan_metrics()["files_listed_by_catalog"] == 3

    def test_tasks_after_pruning_counts_non_none_scan_tasks(self):
        """tasks_after_pruning is lower than files_listed when catalog_scan_task returns None."""
        # Two out of three calls return None → only one task survives.
        side_effects = [Mock(), None, Mock()]
        op = _make_operator(_make_mock_table([100, 200, 300]))
        with patch("daft.io.iceberg.iceberg_scan.ScanTask.catalog_scan_task", side_effect=side_effects):
            with patch.object(op, "_iceberg_record_to_partition_spec", return_value=None):
                list(op._create_regular_scan_tasks(_make_pushdowns()))

        m = get_iceberg_scan_metrics()
        assert m["files_listed_by_catalog"] == 3
        assert m["tasks_after_pruning"] == 2

    def test_bytes_scheduled_sums_file_sizes_of_surviving_tasks(self):
        sizes = [1024, 2048, 4096]
        op = _make_operator(_make_mock_table([10, 20, 30], file_sizes=sizes))
        _run_regular_scan(op)

        assert get_iceberg_scan_metrics()["bytes_scheduled"] == sum(sizes)

    def test_bytes_scheduled_excludes_pruned_tasks(self):
        """Bytes from pruned files (catalog_scan_task returns None) are not counted."""
        sizes = [1024, 2048, 4096]
        # Middle file is pruned.
        side_effects = [Mock(), None, Mock()]
        op = _make_operator(_make_mock_table([10, 20, 30], file_sizes=sizes))
        with patch("daft.io.iceberg.iceberg_scan.ScanTask.catalog_scan_task", side_effect=side_effects):
            with patch.object(op, "_iceberg_record_to_partition_spec", return_value=None):
                list(op._create_regular_scan_tasks(_make_pushdowns()))

        assert get_iceberg_scan_metrics()["bytes_scheduled"] == sizes[0] + sizes[2]

    def test_planning_time_ms_is_non_negative(self):
        op = _make_operator(_make_mock_table([100]))
        _run_regular_scan(op)

        assert get_iceberg_scan_metrics()["planning_time_ms"] >= 0.0

    def test_count_pushdown_used_is_zero_for_regular_scan(self):
        op = _make_operator(_make_mock_table([100]))
        _run_regular_scan(op)

        assert get_iceberg_scan_metrics()["count_pushdown_used"] == 0

    def test_empty_table_all_counts_zero(self):
        op = _make_operator(_make_mock_table([]))
        _run_regular_scan(op)

        m = get_iceberg_scan_metrics()
        assert m["files_listed_by_catalog"] == 0
        assert m["tasks_after_pruning"] == 0
        assert m["bytes_scheduled"] == 0

    def test_reset_clears_stored_metrics(self):
        # Seed a value directly so we can verify reset works independently.
        _mod._tls.last_scan_metrics = {"files_listed_by_catalog": 99}
        reset_iceberg_scan_metrics()
        assert get_iceberg_scan_metrics() == {}

    def test_get_metrics_before_any_scan_returns_empty_dict(self):
        assert get_iceberg_scan_metrics() == {}

    def test_metrics_overwritten_on_each_scan(self):
        """A second scan replaces, not appends to, the previous metrics."""
        op1 = _make_operator(_make_mock_table([100, 200]))
        _run_regular_scan(op1)
        assert get_iceberg_scan_metrics()["files_listed_by_catalog"] == 2

        op2 = _make_operator(_make_mock_table([300]))
        _run_regular_scan(op2)
        assert get_iceberg_scan_metrics()["files_listed_by_catalog"] == 1


class TestCountPushdownMetrics:
    def setup_method(self):
        reset_iceberg_scan_metrics()

    @patch("daft.io.iceberg.iceberg_scan.ScanTask.python_factory_func_scan_task")
    def test_count_pushdown_used_flag_is_set(self, mock_pfst):
        mock_pfst.return_value = Mock()
        op = _make_operator(_make_mock_table([100, 200]))
        list(op._create_count_scan_task(_make_pushdowns(), field_name="count"))

        assert get_iceberg_scan_metrics()["count_pushdown_used"] == 1

    @patch("daft.io.iceberg.iceberg_scan.ScanTask.python_factory_func_scan_task")
    def test_count_pushdown_does_not_set_file_or_byte_metrics(self, mock_pfst):
        """The count-only path doesn't touch files_listed or bytes_scheduled."""
        mock_pfst.return_value = Mock()
        op = _make_operator(_make_mock_table([100, 200]))
        list(op._create_count_scan_task(_make_pushdowns(), field_name="count"))

        m = get_iceberg_scan_metrics()
        assert m["files_listed_by_catalog"] == 0
        assert m["bytes_scheduled"] == 0
