from __future__ import annotations

from unittest.mock import MagicMock, patch

import lance
import pyarrow as pa
import pyarrow.compute as pc
import pytest

import daft
from daft import col
from daft.daft import CountMode
from daft.io.lance.lance_scan import LanceDBScanOperator, _lancedb_count_result_function
from daft.recordbatch import RecordBatch


class TestLanceCountResultFunction:
    """Test that count result function is handled correctly."""

    @pytest.fixture(scope="function")
    def test_dataset_path(self, tmp_path_factory):
        tmp_dir = tmp_path_factory.mktemp("lance_coverage_test")
        test_data = {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"],
            "age": [25, 30, 35, 40, 45, 50],
            "score": [85.5, 92.0, 78.5, 88.0, 95.5, 82.0],
            "active": [True, True, False, True, False, True],
        }
        lance.write_dataset(pa.Table.from_pydict(test_data), tmp_dir)
        yield str(tmp_dir)

    def test_lancedb_count_no_filters_direct_call(self, test_dataset_path):
        """Test that no filters list is handled correctly."""
        ds = lance.dataset(test_dataset_path)
        result_generator = _lancedb_count_result_function(ds, "count")
        result_batch = next(result_generator)
        record_batch = RecordBatch._from_pyrecordbatch(result_batch)
        result_dict = record_batch.to_pydict()
        assert result_dict["count"][0] == 6

    def test_lancedb_count_with_filters_path(self, test_dataset_path):
        """Test that filters list is handled correctly."""
        ds = lance.dataset(test_dataset_path)
        filter_expr = pc.greater(pc.field("age"), pc.scalar(30))
        result_generator = _lancedb_count_result_function(ds, "count", filter=filter_expr)
        result_batch = next(result_generator)
        record_batch = RecordBatch._from_pyrecordbatch(result_batch)
        result_dict = record_batch.to_pydict()
        assert result_dict["count"][0] == 4

    def test_unsupported_count_mode_fallback(self, test_dataset_path):
        """Test that unsupported count mode falls back to regular scan."""
        ds = lance.dataset(test_dataset_path)
        scan_op = LanceDBScanOperator(ds)

        with patch.object(scan_op, "supported_count_modes", return_value=[CountMode.All]):
            with patch("daft.io.lance.lance_scan.logger") as mock_logger:
                mock_pushdowns = MagicMock()
                mock_pushdowns.aggregation = MagicMock()
                mock_pushdowns.aggregation_count_mode.return_value = CountMode.Valid
                mock_pushdowns.aggregation_required_column_names.return_value = ["count"]
                mock_pushdowns.columns = None

                with patch.object(scan_op, "_create_regular_scan_tasks") as mock_regular_scan:
                    mock_regular_scan.return_value = iter([])

                    list(scan_op.to_scan_tasks(mock_pushdowns))

                    mock_logger.warning.assert_called_with(
                        "Count mode %s is not supported for pushdown, falling back to original logic", CountMode.Valid
                    )

                    mock_regular_scan.assert_called_once()

    def test_empty_filters_list_handling(self, test_dataset_path):
        """Test that empty filters list is handled correctly."""
        ds = lance.dataset(test_dataset_path)
        scan_op = LanceDBScanOperator(ds)
        pushed, remaining = scan_op.push_filters([])

        assert len(pushed) == 0
        assert len(remaining) == 0
        assert scan_op._pushed_filters is None

    def test_very_large_filter_expression(self, test_dataset_path):
        """Test that very large filter expressions are handled correctly."""
        df = daft.read_lance(test_dataset_path)

        complex_filter = (col("age") > 20) & (col("age") < 60) & (col("score") > 70) & (col("score") < 100) & (
            col("active")
        ) | ((col("age") > 40) & (col("score") > 90))

        result = df.where(complex_filter).count().collect()
        assert result.to_pydict()["count"][0] == 5
