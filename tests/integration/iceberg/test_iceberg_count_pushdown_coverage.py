from __future__ import annotations

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from daft.daft import CountMode, PyPushdowns
from daft.io.iceberg.iceberg_scan import (
    IcebergScanOperator,
    _iceberg_count_result_function,
)
from daft.logical.schema import Schema


class TestIcebergCountResultFunction:
    """Test suite for _iceberg_count_result_function."""

    def test_iceberg_count_result_function_basic(self):
        """Test basic functionality of _iceberg_count_result_function."""
        total_count = 100
        field_name = "count"

        results = list(_iceberg_count_result_function(total_count, field_name))
        assert len(results) == 1

        result_batch = results[0]
        assert result_batch is not None

    def test_iceberg_count_result_function_zero_count(self):
        """Test _iceberg_count_result_function with zero count."""
        total_count = 0
        field_name = "count"

        results = list(_iceberg_count_result_function(total_count, field_name))
        assert len(results) == 1
        assert results[0] is not None

    def test_iceberg_count_result_function_large_count(self):
        """Test _iceberg_count_result_function with large count."""
        total_count = 2**32 - 1
        field_name = "count"

        results = list(_iceberg_count_result_function(total_count, field_name))
        assert len(results) == 1
        assert results[0] is not None

    def test_iceberg_count_result_function_custom_field_name(self):
        """Test _iceberg_count_result_function with custom field name."""
        total_count = 42
        field_name = "custom_count_field"

        results = list(_iceberg_count_result_function(total_count, field_name))
        assert len(results) == 1
        assert results[0] is not None

    @patch("daft.io.iceberg.iceberg_scan.pa.array")
    def test_iceberg_count_result_function_arrow_error(self, mock_array):
        """Test _iceberg_count_result_function when Arrow operations fail."""
        mock_array.side_effect = Exception("Arrow error")

        total_count = 100
        field_name = "count"

        with pytest.raises(Exception, match="Arrow error"):
            list(_iceberg_count_result_function(total_count, field_name))

    def test_iceberg_count_result_function_empty_field_name(self):
        """Test _iceberg_count_result_function with empty field name."""
        total_count = 100
        field_name = ""

        results = list(_iceberg_count_result_function(total_count, field_name))
        assert len(results) == 1
        assert results[0] is not None


class TestIcebergScanOperatorUnit:
    """Unit tests for IcebergScanOperator methods."""

    def create_mock_iceberg_table(self, has_delete_files=False, record_counts=None):
        """Create a mock Iceberg table for testing."""
        if record_counts is None:
            record_counts = [100]

        mock_table = Mock()
        mock_table.name.return_value = ["test", "table"]

        # Mock schema
        mock_schema = Mock()
        mock_table.schema.return_value = mock_schema

        # Mock spec
        mock_spec = Mock()
        mock_spec.fields = []
        mock_table.spec.return_value = mock_spec

        # Mock scan
        mock_scan = Mock()
        mock_table.scan.return_value = mock_scan
        mock_scan.projection.return_value = mock_schema

        # Mock plan_files with multiple files if specified
        mock_tasks = []
        for i, count in enumerate(record_counts):
            mock_task = Mock()
            mock_file = Mock()
            mock_file.record_count = count
            mock_file.file_path = f"/test/path_{i}.parquet"
            mock_file.file_format = "PARQUET"
            mock_file.file_size_in_bytes = 1024 * (i + 1)
            mock_file.spec_id = 0
            mock_file.partition = []
            mock_task.file = mock_file

            # Configure delete files based on parameter
            if has_delete_files:
                mock_delete_file = Mock()
                mock_delete_file.file_path = f"/test/delete_{i}.parquet"
                mock_task.delete_files = [mock_delete_file]
            else:
                mock_task.delete_files = []

            mock_tasks.append(mock_task)

        mock_scan.plan_files.return_value = mock_tasks

        return mock_table

    @patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow")
    @patch("daft.io.iceberg.iceberg_scan.visit")
    @patch("daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema")
    def test_has_delete_files_no_delete_files(self, mock_from_pyarrow, mock_visit, mock_schema_to_pyarrow):
        """Test _has_delete_files when there are no delete files."""
        # Setup mocks
        mock_schema_to_pyarrow.return_value = pa.schema([pa.field("id", pa.int64())])
        mock_visit.return_value = {}
        mock_from_pyarrow.return_value = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))

        mock_table = self.create_mock_iceberg_table(has_delete_files=False)
        mock_storage_config = Mock()

        operator = IcebergScanOperator(mock_table, None, mock_storage_config)

        # Test the method
        result = operator._has_delete_files()
        assert result is False

    @patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow")
    @patch("daft.io.iceberg.iceberg_scan.visit")
    @patch("daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema")
    def test_has_delete_files_with_delete_files(self, mock_from_pyarrow, mock_visit, mock_schema_to_pyarrow):
        """Test _has_delete_files when there are delete files."""
        # Setup mocks
        mock_schema_to_pyarrow.return_value = pa.schema([pa.field("id", pa.int64())])
        mock_visit.return_value = {}
        mock_from_pyarrow.return_value = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))

        mock_table = self.create_mock_iceberg_table(has_delete_files=True)
        mock_storage_config = Mock()

        operator = IcebergScanOperator(mock_table, None, mock_storage_config)

        # Test the method
        result = operator._has_delete_files()
        assert result is True

    @patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow")
    @patch("daft.io.iceberg.iceberg_scan.visit")
    @patch("daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema")
    def test_supports_count_pushdown_no_delete_files(self, mock_from_pyarrow, mock_visit, mock_schema_to_pyarrow):
        """Test supports_count_pushdown when there are no delete files."""
        # Setup mocks
        mock_schema_to_pyarrow.return_value = pa.schema([pa.field("id", pa.int64())])
        mock_visit.return_value = {}
        mock_from_pyarrow.return_value = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))

        mock_table = self.create_mock_iceberg_table(has_delete_files=False)
        mock_storage_config = Mock()

        operator = IcebergScanOperator(mock_table, None, mock_storage_config)

        # Test the method
        result = operator.supports_count_pushdown()
        assert result is True

    @patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow")
    @patch("daft.io.iceberg.iceberg_scan.visit")
    @patch("daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema")
    def test_supports_count_pushdown_with_delete_files(self, mock_from_pyarrow, mock_visit, mock_schema_to_pyarrow):
        """Test supports_count_pushdown when there are delete files."""
        # Setup mocks
        mock_schema_to_pyarrow.return_value = pa.schema([pa.field("id", pa.int64())])
        mock_visit.return_value = {}
        mock_from_pyarrow.return_value = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))

        mock_table = self.create_mock_iceberg_table(has_delete_files=True)
        mock_storage_config = Mock()

        operator = IcebergScanOperator(mock_table, None, mock_storage_config)

        # Test the method
        result = operator.supports_count_pushdown()
        assert result is False

    @patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow")
    @patch("daft.io.iceberg.iceberg_scan.visit")
    @patch("daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema")
    def test_supported_count_modes(self, mock_from_pyarrow, mock_visit, mock_schema_to_pyarrow):
        """Test supported_count_modes returns correct modes."""
        # Setup mocks
        mock_schema_to_pyarrow.return_value = pa.schema([pa.field("id", pa.int64())])
        mock_visit.return_value = {}
        mock_from_pyarrow.return_value = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))

        mock_table = self.create_mock_iceberg_table()
        mock_storage_config = Mock()

        operator = IcebergScanOperator(mock_table, None, mock_storage_config)

        # Test the method
        result = operator.supported_count_modes()
        assert result == [CountMode.All]

    @patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow")
    @patch("daft.io.iceberg.iceberg_scan.visit")
    @patch("daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema")
    def test_create_count_scan_task_basic(self, mock_from_pyarrow, mock_visit, mock_schema_to_pyarrow):
        """Test _create_count_scan_task basic functionality."""
        # Setup mocks
        mock_schema_to_pyarrow.return_value = pa.schema([pa.field("id", pa.int64())])
        mock_visit.return_value = {}
        mock_from_pyarrow.return_value = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))

        mock_table = self.create_mock_iceberg_table()
        mock_storage_config = Mock()

        operator = IcebergScanOperator(mock_table, None, mock_storage_config)

        # Create mock pushdowns
        mock_pushdowns = Mock(spec=PyPushdowns)

        # Test the method
        with patch("daft.io.iceberg.iceberg_scan.ScanTask.python_factory_func_scan_task") as mock_scan_task:
            mock_scan_task.return_value = Mock()

            results = list(operator._create_count_scan_task(mock_pushdowns, "count"))

            # Should return one scan task
            assert len(results) == 1

            # Verify ScanTask.python_factory_func_scan_task was called
            mock_scan_task.assert_called_once()
            call_args = mock_scan_task.call_args
            assert call_args[1]["func_args"] == (100, "count")  # 100 from mock record_count

    @patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow")
    @patch("daft.io.iceberg.iceberg_scan.visit")
    @patch("daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema")
    def test_create_count_scan_task_multiple_files(self, mock_from_pyarrow, mock_visit, mock_schema_to_pyarrow):
        """Test _create_count_scan_task with multiple data files."""
        # Setup mocks
        mock_schema_to_pyarrow.return_value = pa.schema([pa.field("id", pa.int64())])
        mock_visit.return_value = {}
        mock_from_pyarrow.return_value = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))

        # Create table with multiple files having different counts
        mock_table = self.create_mock_iceberg_table(record_counts=[50, 60, 70])
        mock_storage_config = Mock()

        operator = IcebergScanOperator(mock_table, None, mock_storage_config)

        # Create mock pushdowns
        mock_pushdowns = Mock(spec=PyPushdowns)

        # Test the method
        with patch("daft.io.iceberg.iceberg_scan.ScanTask.python_factory_func_scan_task") as mock_scan_task:
            mock_scan_task.return_value = Mock()

            results = list(operator._create_count_scan_task(mock_pushdowns, "count"))

            # Should return one scan task
            assert len(results) == 1

            # Verify total count is sum of all files: 50 + 60 + 70 = 180
            call_args = mock_scan_task.call_args
            assert call_args[1]["func_args"] == (180, "count")

    @patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow")
    @patch("daft.io.iceberg.iceberg_scan.visit")
    @patch("daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema")
    def test_empty_table_count_pushdown(self, mock_from_pyarrow, mock_visit, mock_schema_to_pyarrow):
        """Test count pushdown on empty table."""
        # Setup mocks
        mock_schema_to_pyarrow.return_value = pa.schema([pa.field("id", pa.int64())])
        mock_visit.return_value = {}
        mock_from_pyarrow.return_value = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))

        # Create empty table
        mock_table = self.create_mock_iceberg_table(record_counts=[])
        mock_storage_config = Mock()

        operator = IcebergScanOperator(mock_table, None, mock_storage_config)

        # Create mock pushdowns
        mock_pushdowns = Mock(spec=PyPushdowns)

        # Test the method
        with patch("daft.io.iceberg.iceberg_scan.ScanTask.python_factory_func_scan_task") as mock_scan_task:
            mock_scan_task.return_value = Mock()

            results = list(operator._create_count_scan_task(mock_pushdowns, "count"))

            # Should return one scan task with count 0
            assert len(results) == 1

            call_args = mock_scan_task.call_args
            assert call_args[1]["func_args"] == (0, "count")  # Empty table should have count 0

    def test_iceberg_count_result_function_negative_count(self):
        """Test _iceberg_count_result_function with negative count (edge case)."""
        total_count = -1
        field_name = "count"

        try:
            results = list(_iceberg_count_result_function(total_count, field_name))
            assert len(results) == 1
        except Exception:
            pass

    @patch("daft.io.iceberg.iceberg_scan.RecordBatch.from_arrow_record_batches")
    def test_iceberg_count_result_function_recordbatch_failure(self, mock_from_arrow):
        """Test _iceberg_count_result_function when RecordBatch conversion fails."""
        mock_from_arrow.side_effect = RuntimeError("RecordBatch conversion failed")

        total_count = 100
        field_name = "count"

        # Should raise the original conversion exception
        with pytest.raises(RuntimeError, match="RecordBatch conversion failed"):
            list(_iceberg_count_result_function(total_count, field_name))

    @patch("daft.io.iceberg.iceberg_scan.pa.field")
    def test_iceberg_count_result_function_field_creation_failure(self, mock_field):
        """Test _iceberg_count_result_function when pa.field creation fails."""
        mock_field.side_effect = pa.ArrowTypeError("Invalid field definition")

        total_count = 100
        field_name = "count"

        with pytest.raises(pa.ArrowTypeError, match="Invalid field definition"):
            list(_iceberg_count_result_function(total_count, field_name))

    @patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow")
    @patch("daft.io.iceberg.iceberg_scan.visit")
    @patch("daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema")
    def test_large_file_count_aggregation(self, mock_from_pyarrow, mock_visit, mock_schema_to_pyarrow):
        """Test count pushdown with many files having large record counts."""
        # Setup mocks
        mock_schema_to_pyarrow.return_value = pa.schema([pa.field("id", pa.int64())])
        mock_visit.return_value = {}
        mock_from_pyarrow.return_value = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))

        # Create table with many files having large counts
        large_counts = [1000000, 2000000, 3000000, 4000000, 5000000]  # 15M total
        mock_table = Mock()
        mock_table.name.return_value = ["test", "large_table"]
        mock_table.schema.return_value = Mock()
        mock_spec = Mock()
        mock_spec.fields = []
        mock_table.spec.return_value = mock_spec

        # Create multiple mock tasks with large counts
        mock_tasks = []
        for i, count in enumerate(large_counts):
            mock_task = Mock()
            mock_file = Mock()
            mock_file.record_count = count
            mock_task.file = mock_file
            mock_task.delete_files = []
            mock_tasks.append(mock_task)

        mock_scan = Mock()
        mock_scan.plan_files.return_value = mock_tasks
        mock_table.scan.return_value = mock_scan

        mock_storage_config = Mock()

        operator = IcebergScanOperator(mock_table, None, mock_storage_config)

        # Create mock pushdowns
        mock_pushdowns = Mock(spec=PyPushdowns)

        # Test the method
        with patch("daft.io.iceberg.iceberg_scan.ScanTask.python_factory_func_scan_task") as mock_scan_task:
            mock_scan_task.return_value = Mock()

            results = list(operator._create_count_scan_task(mock_pushdowns, "count"))

            # Should return one scan task
            assert len(results) == 1

            # Verify total count is sum of all files: 15,000,000
            call_args = mock_scan_task.call_args
            expected_total = sum(large_counts)
            assert call_args[1]["func_args"] == (expected_total, "count")

    @patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow")
    @patch("daft.io.iceberg.iceberg_scan.visit")
    @patch("daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema")
    def test_create_count_scan_task_exception_fallback(self, mock_from_pyarrow, mock_visit, mock_schema_to_pyarrow):
        """Test _create_count_scan_task exception handling and fallback."""
        # Setup mocks
        mock_schema_to_pyarrow.return_value = pa.schema([pa.field("id", pa.int64())])
        mock_visit.return_value = {}
        mock_from_pyarrow.return_value = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))

        # Create table that will fail on scan
        mock_table = Mock()
        mock_table.name.return_value = ["test", "table"]
        mock_table.schema.return_value = Mock()
        mock_spec = Mock()
        mock_spec.fields = []
        mock_table.spec.return_value = mock_spec

        # Mock scan that fails
        mock_scan = Mock()
        mock_scan.plan_files.side_effect = Exception("Iceberg scan failed")
        mock_table.scan.return_value = mock_scan
        mock_scan.projection.return_value = Mock()

        mock_storage_config = Mock()
        operator = IcebergScanOperator(mock_table, None, mock_storage_config)

        # Mock the fallback method
        with patch.object(operator, "_create_regular_scan_tasks") as mock_fallback:
            mock_fallback.return_value = iter([Mock()])

            mock_pushdowns = Mock(spec=PyPushdowns)

            # Test the method - should catch exception and fallback
            results = list(operator._create_count_scan_task(mock_pushdowns, "count"))

            # Should have fallen back to regular scan
            mock_fallback.assert_called_once_with(mock_pushdowns)
            assert len(results) == 1

    @patch("daft.io.iceberg.iceberg_scan.schema_to_pyarrow")
    @patch("daft.io.iceberg.iceberg_scan.visit")
    @patch("daft.io.iceberg.iceberg_scan.Schema.from_pyarrow_schema")
    def test_has_delete_files_exception_handling(self, mock_from_pyarrow, mock_visit, mock_schema_to_pyarrow):
        """Test _has_delete_files exception handling."""
        # Setup mocks
        mock_schema_to_pyarrow.return_value = pa.schema([pa.field("id", pa.int64())])
        mock_visit.return_value = {}
        mock_from_pyarrow.return_value = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))

        # Create table that will fail on scan
        mock_table = Mock()
        mock_table.name.return_value = ["test", "table"]
        mock_table.schema.return_value = Mock()
        mock_spec = Mock()
        mock_spec.fields = []
        mock_table.spec.return_value = mock_spec

        mock_storage_config = Mock()
        operator = IcebergScanOperator(mock_table, None, mock_storage_config)

        result = operator._has_delete_files()

        assert result is True
