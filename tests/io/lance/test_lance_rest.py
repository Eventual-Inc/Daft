# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

import daft
from daft.io.lance.rest_config import LanceRestConfig, parse_lance_uri


class TestLanceRestConfig:
    """Test LanceRestConfig class."""

    def test_rest_config_creation(self):
        """Test creating LanceRestConfig with various parameters."""
        config = LanceRestConfig(base_url="https://api.lancedb.com")
        assert config.base_url == "https://api.lancedb.com"
        assert config.api_key is None
        assert config.timeout == 30
        assert config.headers is None

        config_with_auth = LanceRestConfig(
            base_url="https://api.lancedb.com", api_key="test-key", timeout=60, headers={"Custom-Header": "value"}
        )
        assert config_with_auth.api_key == "test-key"
        assert config_with_auth.timeout == 60
        assert config_with_auth.headers == {"Custom-Header": "value"}


class TestUriParsing:
    """Test URI parsing functionality."""

    def test_parse_file_uri(self):
        """Test parsing file-based URIs."""
        uri_type, uri_info = parse_lance_uri("/path/to/lance/data")
        assert uri_type == "file"
        assert uri_info == {"path": "/path/to/lance/data"}

        uri_type, uri_info = parse_lance_uri("s3://bucket/path/to/data")
        assert uri_type == "file"
        assert uri_info == {"path": "s3://bucket/path/to/data"}

    def test_parse_rest_uri(self):
        """Test parsing REST-based URIs."""
        uri_type, uri_info = parse_lance_uri("rest://namespace/table_name")
        assert uri_type == "rest"
        assert uri_info == {"namespace": "namespace", "table_name": "table_name"}

        # Test with root namespace
        uri_type, uri_info = parse_lance_uri("rest:///table_name")
        assert uri_type == "rest"
        assert uri_info == {"namespace": "$", "table_name": "table_name"}

        # Test with nested table name
        uri_type, uri_info = parse_lance_uri("rest://namespace/schema/table_name")
        assert uri_type == "rest"
        assert uri_info == {"namespace": "namespace", "table_name": "schema/table_name"}

    def test_parse_invalid_rest_uri(self):
        """Test parsing invalid REST URIs."""
        with pytest.raises(ValueError, match="Invalid REST URI format"):
            parse_lance_uri("rest://")

        with pytest.raises(ValueError, match="Invalid REST URI format"):
            parse_lance_uri("rest:///")


@pytest.mark.skipif(
    not pytest.importorskip("lance_namespace", reason="lance_namespace not available"),
    reason="lance_namespace package not available",
)
class TestLanceRestIntegration:
    """Test Lance REST integration with mocked lance_namespace."""

    @patch("lance_namespace.connect")
    def test_read_lance_rest_basic(self, mock_connect):
        """Test basic REST reading functionality."""
        # Mock the REST client
        mock_client = Mock()
        mock_connect.return_value = mock_client

        # Mock table schema
        import pyarrow as pa

        mock_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        mock_table_info = Mock()
        mock_table_info.schema = mock_schema
        mock_client.describe_table.return_value = mock_table_info

        # Mock query result
        mock_result = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        mock_client.query_table.return_value = mock_result

        rest_config = LanceRestConfig(base_url="https://api.lancedb.com", api_key="test-key")

        df = daft.read_lance("rest://test_namespace/test_table", rest_config=rest_config)

        # Verify the client was configured correctly
        mock_connect.assert_called_once_with("rest", {"uri": "https://api.lancedb.com", "api_key": "test-key"})

        # Verify schema is correct
        assert df.schema().column_names() == ["id", "name"]

    @patch("lance_namespace.connect")
    def test_write_lance_rest_create(self, mock_connect):
        """Test REST writing with create mode."""
        # Mock the REST client
        mock_client = Mock()
        mock_connect.return_value = mock_client

        # Mock write result
        mock_result = Mock()
        mock_result.transaction_id = "txn-123"
        mock_client.create_table.return_value = None
        mock_client.insert_records.return_value = mock_result
        mock_client.table_exists.return_value = False

        rest_config = LanceRestConfig(base_url="https://api.lancedb.com", api_key="test-key")

        df = daft.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        result = df.write_lance("rest://test_namespace/test_table", rest_config=rest_config, mode="create")

        # Verify the client was configured correctly
        mock_connect.assert_called_with("rest", {"uri": "https://api.lancedb.com", "api_key": "test-key"})

        # Verify table creation was called
        mock_client.create_table.assert_called_once()
        mock_client.insert_records.assert_called_once()

        # Verify result contains metadata
        assert result is not None

    def test_read_lance_rest_missing_config(self):
        """Test that REST URIs require rest_config."""
        with pytest.raises(ValueError, match="rest_config is required"):
            daft.read_lance("rest://namespace/table")

    def test_write_lance_rest_missing_config(self):
        """Test that REST URIs require rest_config for writing."""
        df = daft.from_pydict({"a": [1, 2, 3]})

        with pytest.raises(ValueError, match="rest_config is required"):
            df.write_lance("rest://namespace/table", mode="create")

    @patch("lance_namespace.connect")
    def test_write_lance_rest_append(self, mock_connect):
        """Test REST writing with append mode."""
        # Mock the REST client
        mock_client = Mock()
        mock_connect.return_value = mock_client

        # Mock write result
        mock_result = Mock()
        mock_result.transaction_id = "txn-456"
        mock_client.insert_records.return_value = mock_result

        rest_config = LanceRestConfig(base_url="https://api.lancedb.com")

        df = daft.from_pydict({"id": [4, 5, 6], "name": ["d", "e", "f"]})

        result = df.write_lance("rest://test_namespace/test_table", rest_config=rest_config, mode="append")

        # Verify insert was called with append mode
        mock_client.insert_records.assert_called_once()

        # Verify result contains metadata
        assert result is not None

    @patch("lance_namespace.connect")
    def test_write_lance_rest_overwrite(self, mock_connect):
        """Test REST writing with overwrite mode."""
        # Mock the REST client
        mock_client = Mock()
        mock_connect.return_value = mock_client

        # Mock write result
        mock_result = Mock()
        mock_result.transaction_id = "txn-789"
        mock_client.drop_table.return_value = None
        mock_client.create_table.return_value = None
        mock_client.insert_records.return_value = mock_result

        rest_config = LanceRestConfig(base_url="https://api.lancedb.com")

        df = daft.from_pydict({"id": [1, 2], "value": [10, 20]})

        result = df.write_lance("rest://test_namespace/test_table", rest_config=rest_config, mode="overwrite")

        # Verify table was dropped and recreated
        mock_client.drop_table.assert_called_once()
        mock_client.create_table.assert_called_once()
        mock_client.insert_records.assert_called_once()

        # Verify result contains metadata
        assert result is not None


class TestLanceRestFallback:
    """Test behavior when lance_namespace is not available."""

    @patch("daft.io.lance.rest_scan.lance_namespace", None)
    def test_read_lance_rest_no_package(self):
        """Test that appropriate error is raised when lance_namespace is not available."""
        rest_config = LanceRestConfig(base_url="https://api.lancedb.com")

        with pytest.raises(ImportError, match="Unable to import the `lance_namespace` package"):
            daft.read_lance("rest://namespace/table", rest_config=rest_config)

    @patch("daft.io.lance.rest_write.lance_namespace", None)
    def test_write_lance_rest_no_package(self):
        """Test that appropriate error is raised when lance_namespace is not available."""
        rest_config = LanceRestConfig(base_url="https://api.lancedb.com")
        df = daft.from_pydict({"a": [1, 2, 3]})

        with pytest.raises(ImportError, match="Unable to import the `lance_namespace` package"):
            df.write_lance("rest://namespace/table", rest_config=rest_config, mode="create")


class TestBackwardCompatibility:
    """Test that existing file-based functionality still works."""

    def test_read_lance_file_unchanged(self):
        """Test that file-based read_lance still works without rest_config."""
        # This should not raise an error about missing rest_config
        try:
            # This will fail because the file doesn't exist, but it should not fail due to REST config
            daft.read_lance("/nonexistent/path")
        except Exception as e:
            # Should be a file-related error, not a REST config error
            assert "rest_config" not in str(e)

    def test_write_lance_file_unchanged(self):
        """Test that file-based write_lance still works without rest_config."""
        df = daft.from_pydict({"a": [1, 2, 3]})

        # This should not raise an error about missing rest_config
        try:
            # This will fail because we can't actually write, but it should not fail due to REST config
            df.write_lance("/nonexistent/path", mode="create")
        except Exception as e:
            # Should be a file-related error, not a REST config error
            assert "rest_config" not in str(e)
