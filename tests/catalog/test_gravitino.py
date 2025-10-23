"""Basic tests for Gravitino client functionality."""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
import requests

from daft.gravitino import GravitinoCatalog, GravitinoClient, GravitinoTable


def test_gravitino_client_init():
    """Test GravitinoClient initialization."""
    # Test simple auth
    client = GravitinoClient(
        endpoint="http://localhost:8090", metalake_name="test_metalake", auth_type="simple", username="admin"
    )
    assert client._endpoint == "http://localhost:8090"
    assert client._metalake_name == "test_metalake"
    assert client._auth_type == "simple"
    assert client._username == "admin"

    # Test OAuth2 auth
    client = GravitinoClient(
        endpoint="http://localhost:8090/",  # Test trailing slash removal
        metalake_name="test_metalake",
        auth_type="oauth2",
        token="test-token",
    )
    assert client._endpoint == "http://localhost:8090"
    assert client._metalake_name == "test_metalake"
    assert client._auth_type == "oauth2"
    assert client._token == "test-token"


@patch("requests.Session.request")
def test_list_metalakes(mock_request):
    """Test listing metalakes."""
    mock_response = Mock()
    mock_response.json.return_value = {"metalakes": [{"name": "metalake1"}, {"name": "metalake2"}]}
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")
    metalakes = client.list_metalakes()

    assert metalakes == ["metalake1", "metalake2"]
    mock_request.assert_called_once()


@patch("requests.Session.request")
def test_list_catalogs(mock_request):
    """Test listing catalogs."""
    mock_response = Mock()
    mock_response.json.return_value = {"catalogs": [{"name": "catalog1"}, {"name": "catalog2"}]}
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")
    catalogs = client.list_catalogs()

    assert catalogs == ["catalog1", "catalog2"]
    mock_request.assert_called_once()


@patch("requests.Session.request")
def test_load_catalog(mock_request):
    """Test loading a catalog."""
    mock_response = Mock()
    mock_response.json.return_value = {
        "catalog": {
            "name": "test_catalog",
            "type": "relational",
            "provider": "hive",
            "properties": {
                "metastore.uris": "thrift://localhost:9083",
                "warehouse": "hdfs://localhost:9000/user/hive/warehouse",
            },
        }
    }
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")
    catalog = client.load_catalog("test_catalog")

    assert isinstance(catalog, GravitinoCatalog)
    assert catalog.name == "test_catalog"
    assert catalog.type == "relational"
    assert catalog.provider == "hive"
    assert catalog.properties["metastore.uris"] == "thrift://localhost:9083"


@patch("requests.Session.request")
def test_load_nonexistent_catalog(mock_request):
    """Test loading a non-existent catalog throws exception."""
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=Mock(status_code=404))
    mock_request.return_value = mock_response

    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")

    with pytest.raises(Exception, match="Catalog .* not found"):
        client.load_catalog("nonexistent_catalog")


@patch("requests.Session.request")
def test_list_schemas(mock_request):
    """Test listing schemas."""
    mock_response = Mock()
    mock_response.json.return_value = {"schemas": [{"name": "schema1"}, {"name": "schema2"}]}
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")
    schemas = client.list_schemas("catalog1")

    assert schemas == ["catalog1.schema1", "catalog1.schema2"]


@patch("requests.Session.request")
def test_list_tables(mock_request):
    """Test listing tables."""
    mock_response = Mock()
    mock_response.json.return_value = {"tables": [{"name": "table1"}, {"name": "table2"}]}
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")
    tables = client.list_tables("catalog1.schema1")

    assert tables == ["catalog1.schema1.table1", "catalog1.schema1.table2"]


def test_invalid_table_name():
    """Test error handling for invalid table names."""
    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")

    with pytest.raises(ValueError, match="Expected table name format"):
        client.load_table("invalid_name")


def test_invalid_schema_name():
    """Test error handling for invalid schema names."""
    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")

    with pytest.raises(ValueError, match="Expected fully-qualified schema name"):
        client.list_tables("invalid_schema")


@patch("requests.Session.request")
def test_load_existing_table(mock_request):
    """Test loading an existing table."""
    mock_response = Mock()
    mock_response.json.return_value = {
        "table": {
            "name": "test_table",
            "type": "EXTERNAL",
            "properties": {
                "location": "s3://bucket/path/",
                "format": "DELTA",
                "s3.access-key-id": "test-key",
                "s3.secret-access-key": "test-secret",
            },
        }
    }
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")
    table = client.load_table("catalog1.schema1.test_table")

    assert isinstance(table, GravitinoTable)
    assert table.table_info.name == "test_table"
    assert table.table_info.format == "DELTA"
    assert table.table_uri == "s3://bucket/path/"
    assert table.io_config is not None


@patch("requests.Session.request")
def test_load_nonexistent_table(mock_request):
    """Test loading a non-existent table throws exception."""
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=Mock(status_code=404))
    mock_request.return_value = mock_response

    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")

    with pytest.raises(Exception, match="Table .* not found"):
        client.load_table("catalog1.schema1.nonexistent_table")


if __name__ == "__main__":
    # Run basic tests
    test_gravitino_client_init()
    print("✓ Client initialization test passed")

    test_invalid_table_name()
    print("✓ Invalid table name test passed")

    test_invalid_schema_name()
    print("✓ Invalid schema name test passed")

    print("All basic tests passed!")
