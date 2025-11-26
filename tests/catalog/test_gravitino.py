"""Basic tests for Gravitino client functionality."""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
import requests

from daft.catalog import Identifier, NotFoundError, Schema
from daft.catalog.__gravitino import GravitinoCatalog as CatalogWrapper
from daft.catalog.__gravitino import GravitinoTable as TableWrapper
from daft.gravitino import GravitinoCatalog, GravitinoClient, GravitinoTable
from daft.gravitino import GravitinoTable as InnerTable


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
def test_list_catalogs(mock_request):
    """Test listing catalogs."""
    mock_response = Mock()
    mock_response.json.return_value = {
        "code": 0,
        "identifiers": [
            {"namespace": ["my_metalake"], "name": "catalog1"},
            {"namespace": ["my_metalake"], "name": "catalog2"},
        ],
    }
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
    mock_response.json.return_value = {
        "code": 0,
        "identifiers": [
            {"namespace": ["my_metalake", "catalog1"], "name": "schema1"},
            {"namespace": ["my_metalake", "catalog1"], "name": "schema2"},
        ],
    }
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")
    schemas = client.list_schemas("catalog1")

    assert schemas == ["catalog1.schema1", "catalog1.schema2"]


@patch("requests.Session.request")
def test_list_tables(mock_request):
    """Test listing tables."""
    mock_response = Mock()
    mock_response.json.return_value = {
        "code": 0,
        "identifiers": [
            {"namespace": ["my_metalake", "catalog1", "schema1"], "name": "table1"},
            {"namespace": ["my_metalake", "catalog1", "schema1"], "name": "table2"},
        ],
    }
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
                "format": "ICEBERG",
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
    assert table.table_info.format == "ICEBERG"
    assert table.table_uri == "s3://bucket/path/"
    # assert table.io_config is not None


@patch("requests.Session.request")
def test_load_nonexistent_table(mock_request):
    """Test loading a non-existent table throws exception."""
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=Mock(status_code=404))
    mock_request.return_value = mock_response

    client = GravitinoClient("http://localhost:8090", "test_metalake", username="admin")

    with pytest.raises(Exception, match="Table .* not found"):
        client.load_table("catalog1.schema1.nonexistent_table")


class TestGravitinoCatalog:
    """Test GravitinoCatalog class from daft/catalog/__gravitino.py."""

    @pytest.fixture
    def mock_inner_catalog(self):
        """Create a mock InnerCatalog (GravitinoClient) for testing."""
        mock_catalog = Mock(spec=GravitinoClient)
        mock_catalog._metalake_name = "test_metalake"
        return mock_catalog

    @pytest.fixture
    def gravitino_catalog(self, mock_inner_catalog):
        """Create a GravitinoCatalog instance for testing."""
        catalog = CatalogWrapper._from_obj(mock_inner_catalog)
        return catalog

    def test_init_raises_error(self):
        """Test that direct __init__ raises RuntimeError."""
        with pytest.raises(RuntimeError, match="GravitinoCatalog.__init__ is not supported"):
            CatalogWrapper()

    def test_from_obj_with_valid_object(self, mock_inner_catalog):
        """Test _from_obj with valid InnerCatalog object."""
        catalog = CatalogWrapper._from_obj(mock_inner_catalog)
        assert isinstance(catalog, CatalogWrapper)
        assert catalog._inner is mock_inner_catalog

    def test_from_obj_with_invalid_object(self):
        """Test _from_obj with invalid object raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported gravitino catalog type"):
            CatalogWrapper._from_obj("invalid_object")

    def test_name_property(self, gravitino_catalog):
        """Test name property returns correct format."""
        assert gravitino_catalog.name == "gravitino_test_metalake"

    def test_create_namespace_not_implemented(self, gravitino_catalog):
        """Test _create_namespace raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Gravitino create_namespace not yet supported"):
            gravitino_catalog._create_namespace(Identifier.from_str("test_namespace"))

    def test_create_table_not_implemented(self, gravitino_catalog):
        """Test _create_table raises NotImplementedError."""
        # Create a minimal schema mock
        mock_schema = Mock(spec=Schema)

        with pytest.raises(NotImplementedError, match="Gravitino create_table not yet supported"):
            gravitino_catalog._create_table(Identifier.from_str("test_table"), source=mock_schema)

    def test_drop_namespace_not_implemented(self, gravitino_catalog):
        """Test _drop_namespace raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Gravitino drop_namespace not yet supported"):
            gravitino_catalog._drop_namespace(Identifier.from_str("test_namespace"))

    def test_drop_table_not_implemented(self, gravitino_catalog):
        """Test _drop_table raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Gravitino drop_table not yet supported"):
            gravitino_catalog._drop_table(Identifier.from_str("test_table"))

    def test_get_table_success(self, gravitino_catalog, mock_inner_catalog):
        """Test _get_table successfully loads a table."""
        mock_table = Mock(spec=InnerTable)
        mock_inner_catalog.load_table.return_value = mock_table

        result = gravitino_catalog._get_table(Identifier.from_str("catalog.schema.table"))

        assert isinstance(result, TableWrapper)
        mock_inner_catalog.load_table.assert_called_once_with("catalog.schema.table")

    def test_get_table_not_found(self, gravitino_catalog, mock_inner_catalog):
        """Test _get_table raises NotFoundError when table not found."""
        mock_inner_catalog.load_table.side_effect = Exception("Table not found in catalog")

        with pytest.raises(NotFoundError, match="Table .* not found"):
            gravitino_catalog._get_table(Identifier.from_str("catalog.schema.nonexistent"))

    def test_get_table_other_exception(self, gravitino_catalog, mock_inner_catalog):
        """Test _get_table propagates non-NotFound exceptions."""
        mock_inner_catalog.load_table.side_effect = Exception("Connection error")

        with pytest.raises(Exception, match="Connection error"):
            gravitino_catalog._get_table(Identifier.from_str("catalog.schema.table"))

    def test_list_namespaces_not_implemented(self, gravitino_catalog):
        """Test _list_namespaces raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Gravitino list_namespaces not yet supported"):
            gravitino_catalog._list_namespaces()

    def test_list_tables_no_pattern(self, gravitino_catalog, mock_inner_catalog):
        """Test _list_tables with no pattern lists all tables."""
        mock_inner_catalog.list_catalogs.return_value = ["catalog1", "catalog2"]
        mock_inner_catalog.list_schemas.side_effect = lambda cat: [f"{cat}.schema1", f"{cat}.schema2"]
        mock_inner_catalog.list_tables.side_effect = lambda schema: [f"{schema}.table1", f"{schema}.table2"]

        result = gravitino_catalog._list_tables()

        assert len(result) == 8  # 2 catalogs * 2 schemas * 2 tables
        assert all(isinstance(ident, Identifier) for ident in result)

    def test_list_tables_with_catalog_pattern(self, gravitino_catalog, mock_inner_catalog):
        """Test _list_tables with catalog pattern."""
        mock_inner_catalog.list_schemas.return_value = ["catalog1.schema1", "catalog1.schema2"]
        mock_inner_catalog.list_tables.side_effect = lambda schema: [f"{schema}.table1", f"{schema}.table2"]

        result = gravitino_catalog._list_tables(pattern="catalog1")

        assert len(result) == 4  # 2 schemas * 2 tables
        assert all(isinstance(ident, Identifier) for ident in result)

    def test_list_tables_with_schema_pattern(self, gravitino_catalog, mock_inner_catalog):
        """Test _list_tables with schema pattern."""
        mock_inner_catalog.list_tables.return_value = ["catalog1.schema1.table1", "catalog1.schema1.table2"]

        result = gravitino_catalog._list_tables(pattern="catalog1.schema1")

        assert len(result) == 2
        assert all(isinstance(ident, Identifier) for ident in result)

    def test_list_tables_with_invalid_pattern(self, gravitino_catalog):
        """Test _list_tables with invalid pattern raises ValueError."""
        with pytest.raises(ValueError, match="Unrecognized catalog name or schema name"):
            gravitino_catalog._list_tables(pattern="catalog.schema.table.extra")

    def test_list_tables_with_empty_pattern(self, gravitino_catalog, mock_inner_catalog):
        """Test _list_tables with empty string pattern."""
        mock_inner_catalog.list_catalogs.return_value = ["catalog1"]
        mock_inner_catalog.list_schemas.return_value = ["catalog1.schema1"]
        mock_inner_catalog.list_tables.return_value = ["catalog1.schema1.table1"]

        result = gravitino_catalog._list_tables(pattern="")

        assert len(result) == 1

    def test_has_namespace_not_implemented(self, gravitino_catalog):
        """Test _has_namespace raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Gravitino has_namespace not yet supported"):
            gravitino_catalog._has_namespace(Identifier.from_str("test_namespace"))

    def test_has_table_exists(self, gravitino_catalog, mock_inner_catalog):
        """Test _has_table returns True when table exists."""
        mock_inner_catalog.load_table.return_value = Mock()

        result = gravitino_catalog._has_table(Identifier.from_str("catalog.schema.table"))

        assert result is True

    def test_has_table_not_exists(self, gravitino_catalog, mock_inner_catalog):
        """Test _has_table returns False when table doesn't exist."""
        mock_inner_catalog.load_table.side_effect = Exception("Table not found")

        result = gravitino_catalog._has_table(Identifier.from_str("catalog.schema.nonexistent"))

        assert result is False


class TestGravitinoTable:
    """Test GravitinoTable class from daft/catalog/__gravitino.py."""

    @pytest.fixture
    def mock_inner_table(self):
        """Create a mock InnerTable (GravitinoTable from gravitino module) for testing."""
        mock_table = Mock(spec=InnerTable)
        mock_table.table_info = Mock()
        mock_table.table_info.name = "test_table"
        mock_table.table_info.format = "ICEBERG"
        mock_table.table_uri = "s3://bucket/path/metadata.json"
        mock_table.io_config = Mock()
        return mock_table

    @pytest.fixture
    def gravitino_table(self, mock_inner_table):
        """Create a GravitinoTable instance for testing."""
        table = TableWrapper._from_obj(mock_inner_table)
        return table

    def test_init_raises_error(self):
        """Test that direct __init__ raises RuntimeError."""
        with pytest.raises(RuntimeError, match="GravitinoTable.__init__ is not supported"):
            TableWrapper()

    def test_from_obj_with_valid_object(self, mock_inner_table):
        """Test _from_obj with valid InnerTable object."""
        table = TableWrapper._from_obj(mock_inner_table)
        assert isinstance(table, TableWrapper)
        assert table._inner is mock_inner_table

    def test_from_obj_with_invalid_object(self):
        """Test _from_obj with invalid object raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported gravitino table type"):
            TableWrapper._from_obj("invalid_object")

    def test_name_property(self, gravitino_table):
        """Test name property returns table name."""
        assert gravitino_table.name == "test_table"

    def test_read_options_attribute(self):
        """Test _read_options class attribute."""
        assert "snapshot_id" in TableWrapper._read_options

    def test_write_options_attribute(self):
        """Test _write_options class attribute."""
        assert isinstance(TableWrapper._write_options, set)

    @patch("daft.catalog.__gravitino.read_iceberg")
    def test_read_iceberg_table(self, mock_read_iceberg, gravitino_table):
        """Test reading an Iceberg table."""
        mock_df = Mock()
        mock_read_iceberg.return_value = mock_df

        result = gravitino_table.read()

        assert result is mock_df
        mock_read_iceberg.assert_called_once_with(
            table=gravitino_table._inner.table_uri,
            snapshot_id=None,
            io_config=gravitino_table._inner.io_config,
        )

    @patch("daft.catalog.__gravitino.read_iceberg")
    def test_read_iceberg_table_with_snapshot_id(self, mock_read_iceberg, gravitino_table):
        """Test reading an Iceberg table with snapshot_id option."""
        mock_df = Mock()
        mock_read_iceberg.return_value = mock_df

        result = gravitino_table.read(snapshot_id=12345)

        assert result is mock_df
        mock_read_iceberg.assert_called_once_with(
            table=gravitino_table._inner.table_uri,
            snapshot_id=12345,
            io_config=gravitino_table._inner.io_config,
        )

    @patch("daft.catalog.__gravitino.read_iceberg")
    def test_read_iceberg_table_pyiceberg_not_installed(self, mock_read_iceberg, gravitino_table):
        """Test reading Iceberg table when pyiceberg is not installed."""
        mock_read_iceberg.side_effect = ImportError("No module named 'pyiceberg'")

        with pytest.raises(ImportError, match="PyIceberg is required"):
            gravitino_table.read()

    def test_read_non_iceberg_table(self, mock_inner_table):
        """Test reading a non-Iceberg table raises NotImplementedError."""
        mock_inner_table.table_info.format = "PARQUET"
        table = TableWrapper._from_obj(mock_inner_table)

        with pytest.raises(NotImplementedError, match="Reading PARQUET format tables is not yet supported"):
            table.read()

    def test_read_with_invalid_option(self, gravitino_table):
        """Test read with invalid option raises error."""
        with pytest.raises(ValueError, match="Unsupported option"):
            gravitino_table.read(invalid_option="value")

    def test_schema_calls_read(self, gravitino_table):
        """Test schema() method calls read().schema()."""
        mock_df = Mock()
        mock_schema = Mock()
        mock_df.schema.return_value = mock_schema

        with patch.object(gravitino_table, "read", return_value=mock_df):
            result = gravitino_table.schema()

            assert result is mock_schema
            gravitino_table.read.assert_called_once()

    def test_append_not_implemented(self, gravitino_table):
        """Test append raises NotImplementedError."""
        mock_df = Mock()

        with pytest.raises(NotImplementedError, match="Writing to Iceberg tables through Gravitino"):
            gravitino_table.append(mock_df)

    def test_append_non_iceberg_not_implemented(self, mock_inner_table):
        """Test append on non-Iceberg table raises NotImplementedError."""
        mock_inner_table.table_info.format = "PARQUET"
        table = TableWrapper._from_obj(mock_inner_table)
        mock_df = Mock()

        with pytest.raises(NotImplementedError, match="Writing PARQUET format tables is not yet supported"):
            table.append(mock_df)

    def test_overwrite_not_implemented(self, gravitino_table):
        """Test overwrite raises NotImplementedError."""
        mock_df = Mock()

        with pytest.raises(NotImplementedError, match="Writing to Iceberg tables through Gravitino"):
            gravitino_table.overwrite(mock_df)

    def test_overwrite_non_iceberg_not_implemented(self, mock_inner_table):
        """Test overwrite on non-Iceberg table raises NotImplementedError."""
        mock_inner_table.table_info.format = "DELTA"
        table = TableWrapper._from_obj(mock_inner_table)
        mock_df = Mock()

        with pytest.raises(NotImplementedError, match="Writing DELTA format tables is not yet supported"):
            table.overwrite(mock_df)

    def test_read_iceberg_variant_format(self, mock_inner_table):
        """Test reading Iceberg variant formats (e.g., ICEBERG/PARQUET)."""
        mock_inner_table.table_info.format = "ICEBERG/PARQUET"
        table = TableWrapper._from_obj(mock_inner_table)

        with patch("daft.catalog.__gravitino.read_iceberg") as mock_read:
            mock_read.return_value = Mock()
            table.read()
            mock_read.assert_called_once()

    def test_append_with_invalid_option(self, gravitino_table):
        """Test append with invalid option raises error."""
        mock_df = Mock()

        # First it should validate options before hitting NotImplementedError
        with pytest.raises((ValueError, NotImplementedError)):
            gravitino_table.append(mock_df, invalid_option="value")

    def test_overwrite_with_invalid_option(self, gravitino_table):
        """Test overwrite with invalid option raises error."""
        mock_df = Mock()

        # First it should validate options before hitting NotImplementedError
        with pytest.raises((ValueError, NotImplementedError)):
            gravitino_table.overwrite(mock_df, invalid_option="value")


if __name__ == "__main__":
    # Run basic tests
    test_gravitino_client_init()
    print("Client initialization test passed")

    test_invalid_table_name()
    print("Invalid table name test passed")

    test_invalid_schema_name()
    print("Invalid schema name test passed")

    print("All basic tests passed!")
