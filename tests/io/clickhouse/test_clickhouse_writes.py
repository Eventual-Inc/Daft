from __future__ import annotations

import unittest
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from clickhouse_connect.driver.summary import QuerySummary

import daft
from daft.dataframe.dataframe import DataFrame
from daft.io.clickhouse.clickhouse_data_sink import ClickHouseDataSink


class TestWriteClickHouse:
    @pytest.fixture
    def sample_data(self):
        return daft.from_pydict(
            {
                "vector": [[1.1, 1.2], [0.2, 1.8]],
                "lat": [45.5, 40.1],
                "long": [-122.7, -74.1],
            }
        )

    @patch.object(ClickHouseDataSink, "__init__", return_value=None)
    @patch.object(DataFrame, "write_sink")
    def test_write_clickhouse_minimal_params(self, mock_write_sink, mock_clickhouse_sink, sample_data):
        """Test minimal parameters for write_clickhouse."""
        sample_data.write_clickhouse(table="test_table", host="localhost")

        mock_clickhouse_sink.assert_called_once_with(
            "test_table",
            host="localhost",
            port=None,
            user=None,
            password=None,
            database=None,
            client_kwargs=None,
            write_kwargs=None,
        )
        mock_write_sink.assert_called_once()

    @patch.object(ClickHouseDataSink, "__init__", return_value=None)
    @patch.object(DataFrame, "write_sink")
    def test_write_clickhouse_all_params(self, mock_write_sink, mock_clickhouse_sink, sample_data):
        """Test all parameters for write_clickhouse."""
        sample_data.write_clickhouse(
            table="test_table",
            host="localhost",
            port=8123,
            user="user",
            password="pass",
            database="db",
            client_kwargs={"timeout": 10},
            write_kwargs={"batch_size": 1000},
        )

        mock_clickhouse_sink.assert_called_once_with(
            "test_table",
            host="localhost",
            port=8123,
            user="user",
            password="pass",
            database="db",
            client_kwargs={"timeout": 10},
            write_kwargs={"batch_size": 1000},
        )
        mock_write_sink.assert_called_once()

    @patch.object(ClickHouseDataSink, "__init__", return_value=None)
    @patch.object(DataFrame, "write_sink", side_effect=Exception("Write error"))
    def test_write_clickhouse_write_error(self, mock_write_sink, mock_clickhouse_sink, sample_data):
        """Test write error for write_clickhouse."""
        with pytest.raises(Exception, match="Write error"):
            sample_data.write_clickhouse(table="test_table", host="localhost")


class TestClickHouseDataSink:
    """Tests for ClickHouseDataSink class."""

    @pytest.fixture
    def clickhouse_sink(self):
        return ClickHouseDataSink(
            table="test_table", host="localhost", port=8123, user="user", password="pass", database="db"
        )

    def test_initialization(self, clickhouse_sink):
        assert clickhouse_sink._table == "test_table"
        assert clickhouse_sink._client_kwargs["host"] == "localhost"
        assert clickhouse_sink._client_kwargs["port"] == 8123
        assert clickhouse_sink._client_kwargs["user"] == "user"
        assert clickhouse_sink._client_kwargs["password"] == "pass"
        assert clickhouse_sink._client_kwargs["database"] == "db"

    @pytest.fixture
    def mock_client(self):
        with patch("daft.io.clickhouse.clickhouse_data_sink.get_client") as mock:  # 修正mock路径
            client = MagicMock()
            client.insert_df.return_value = QuerySummary(
                summary={"written_rows": 3, "written_bytes": 128, "query_id": "test_query"}
            )
            client.command.return_value = ("22.8.0", "UTC")
            mock.return_value = client
            yield client

    def test_clickhouse_sink_write(self, mock_client):
        sink = ClickHouseDataSink(
            table="test_table",
            host="localhost",
            client_kwargs={"settings": {"async_insert": 1}},
            write_kwargs={"column_names": ["col1"]},
        )

        mp = daft.recordbatch.MicroPartition.from_pydict({"col1": [1, 2, 3]})
        results = list(sink.write(iter([mp])))

        mock_client.insert_df.assert_called_once_with("test_table", unittest.mock.ANY, column_names=["col1"])
        pd.testing.assert_frame_equal(mock_client.insert_df.call_args[0][1], mp.to_pandas())

        assert len(results) == 1
        assert isinstance(results[0].result, QuerySummary)

    def test_finalize_statistics(self):
        sink = ClickHouseDataSink(table="test", host="localhost")
        mock_results = [Mock(rows_written=10, bytes_written=100), Mock(rows_written=20, bytes_written=200)]

        result = sink.finalize(mock_results)
        assert result.to_pydict() == {"total_written_rows": [30], "total_written_bytes": [300]}

    def test_client_cleanup(self, mock_client):
        sink = ClickHouseDataSink(table="test", host="localhost")
        mp = daft.recordbatch.MicroPartition.from_pydict({"col1": [1]})

        mock_client.insert_df.side_effect = Exception("DB error")
        with pytest.raises(Exception):
            list(sink.write(iter([mp])))

        mock_client.close.assert_called_once()
