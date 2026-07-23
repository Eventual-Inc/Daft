"""Tests for ClickHouse-specific percentile dialect in SQLScanOperator."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa

from daft.sql.sql_scan import PartitionBoundStrategy, SQLScanOperator


class TestClickHousePercentile:
    """Test ClickHouse-specific percentile syntax."""

    def test_clickhouse_uses_quantile_exact(self):
        """ClickHouse should use quantileExact()() syntax in partition bounds."""
        conn = MagicMock()
        conn.dialect = "clickhouse"
        conn.driver = ""
        conn.url = "clickhouse://default:@localhost/default"

        # construct_sql_query returns a plain string (we don't care about its value)
        conn.construct_sql_query.return_value = "SELECT ..."
        # execute_sql_query returns a table with 3 bound columns for 2 scan tasks
        conn.execute_sql_query.return_value = pa.table({"bound_0": [0], "bound_1": [50], "bound_2": [100]})

        schema = {"id": MagicMock(is_numeric=MagicMock(return_value=True), is_temporal=MagicMock(return_value=False))}

        op = SQLScanOperator.__new__(SQLScanOperator)
        op.sql = "SELECT * FROM t"
        op.conn = conn
        op._partition_col = "id"
        op._partition_bound_strategy = PartitionBoundStrategy.PERCENTILE
        op._schema = schema

        op._get_partition_bounds(num_scan_tasks=2)

        # Verify construct_sql_query was called with quantileExact in the projection
        call_kwargs = conn.construct_sql_query.call_args
        projection = call_kwargs.kwargs.get("projection") or call_kwargs[1].get("projection")
        assert projection is not None
        assert all("quantileExact(" in p for p in projection), f"Expected quantileExact, got: {projection}"
        assert not any(p.startswith("quantile(") and "quantileExact" not in p for p in projection)

    def test_standard_dialect_uses_percentile_disc(self):
        """Standard SQL dialects should use percentile_disc syntax."""
        conn = MagicMock()
        conn.dialect = "postgres"
        conn.driver = ""
        conn.url = "postgresql://localhost/test"

        # construct_sql_query returns a plain string (we don't care about its value)
        conn.construct_sql_query.return_value = "SELECT ..."
        # execute_sql_query returns a table with 3 bound columns for 2 scan tasks
        conn.execute_sql_query.return_value = pa.table({"bound_0": [0], "bound_1": [50], "bound_2": [100]})

        schema = {"id": MagicMock(is_numeric=MagicMock(return_value=True), is_temporal=MagicMock(return_value=False))}

        op = SQLScanOperator.__new__(SQLScanOperator)
        op.sql = "SELECT * FROM t"
        op.conn = conn
        op._partition_col = "id"
        op._partition_bound_strategy = PartitionBoundStrategy.PERCENTILE
        op._schema = schema

        op._get_partition_bounds(num_scan_tasks=2)

        # Verify construct_sql_query was called with percentile_disc in the projection
        call_kwargs = conn.construct_sql_query.call_args
        projection = call_kwargs.kwargs.get("projection") or call_kwargs[1].get("projection")
        assert projection is not None
        assert all("percentile_disc(" in p for p in projection), f"Expected percentile_disc, got: {projection}"
        assert not any("quantileExact(" in p for p in projection), (
            f"Should not use quantileExact for postgres, got: {projection}"
        )
