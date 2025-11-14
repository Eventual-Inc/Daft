#!/usr/bin/env python3
"""Integration test to verify Gravitino catalog integration works correctly."""

from __future__ import annotations

import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

import pytest

from daft.catalog import Catalog, Table
from daft.gravitino import GravitinoClient, GravitinoTable, GravitinoTableInfo


class TestGravitinoIntegration:
    """Integration tests for Gravitino catalog functionality."""

    def test_catalog_from_gravitino(self):
        """Test that Catalog.from_gravitino() works."""
        # Create a Gravitino client
        client = GravitinoClient(
            endpoint="http://localhost:8090", metalake_name="metalake_demo", auth_type="simple", username="admin"
        )

        # Create catalog from Gravitino client
        catalog = Catalog.from_gravitino(client)

        assert catalog is not None
        assert catalog.name == "gravitino_metalake_demo"
        assert "GravitinoCatalog" in str(type(catalog))

    def test_table_from_gravitino(self):
        """Test that Table.from_gravitino() works."""
        # Create a mock Gravitino table
        table_info = GravitinoTableInfo(
            name="test_table",
            catalog="test_catalog",
            schema="test_schema",
            table_type="EXTERNAL",
            storage_location="s3://test-bucket/path/",
            format="ICEBERG",
            properties={"format": "ICEBERG"},
        )

        gravitino_table = GravitinoTable(table_info=table_info, table_uri="s3://test-bucket/path/", io_config=None)

        # Create table from Gravitino table
        table = Table.from_gravitino(gravitino_table)

        assert table is not None
        assert table.name == "test_table"
        assert "GravitinoTable" in str(type(table))

    def test_catalog_has_table_method(self):
        """Test catalog has_table method works."""
        client = GravitinoClient(
            endpoint="http://localhost:8090", metalake_name="metalake_demo", auth_type="simple", username="admin"
        )

        catalog = Catalog.from_gravitino(client)

        # Test has_table method (should not crash)
        result = catalog.has_table("nonexistent.table")
        assert isinstance(result, bool)
        assert result is False  # Should be False for nonexistent table

    def test_catalog_list_tables_method(self):
        """Test catalog list_tables method works."""
        client = GravitinoClient(
            endpoint="http://localhost:8090", metalake_name="metalake_demo", auth_type="simple", username="admin"
        )

        catalog = Catalog.from_gravitino(client)

        # Test list_tables method (should not crash)
        tables = catalog.list_tables()
        assert isinstance(tables, list)
        # Should be empty list when no server is running

    def test_catalog_get_table_not_found(self):
        """Test catalog get_table raises NotFoundError for nonexistent table."""
        from daft.catalog import NotFoundError

        client = GravitinoClient(
            endpoint="http://localhost:8090", metalake_name="metalake_demo", auth_type="simple", username="admin"
        )

        catalog = Catalog.from_gravitino(client)

        # Should raise NotFoundError for nonexistent table
        with pytest.raises(NotFoundError):
            catalog.get_table("nonexistent.schema.table")

    def test_table_properties(self):
        """Test that GravitinoTable has expected properties."""
        table_info = GravitinoTableInfo(
            name="test_table",
            catalog="test_catalog",
            schema="test_schema",
            table_type="EXTERNAL",
            storage_location="s3://test-bucket/path/",
            format="ICEBERG",
            properties={"format": "ICEBERG", "key": "value"},
        )

        gravitino_table = GravitinoTable(table_info=table_info, table_uri="s3://test-bucket/path/", io_config=None)

        table = Table.from_gravitino(gravitino_table)

        # Test table properties
        assert table.name == "test_table"
        assert hasattr(table, "_inner")
        assert table._inner.table_info.catalog == "test_catalog"
        assert table._inner.table_info.schema == "test_schema"
        assert table._inner.table_uri == "s3://test-bucket/path/"

    def test_catalog_factory_methods_exist(self):
        """Test that factory methods exist on Catalog and Table classes."""
        # Test Catalog factory methods
        assert hasattr(Catalog, "from_gravitino")
        assert callable(getattr(Catalog, "from_gravitino"))

        # Test Table factory methods
        assert hasattr(Table, "from_gravitino")
        assert callable(getattr(Table, "from_gravitino"))

    def test_catalog_from_obj_includes_gravitino(self):
        """Test that Catalog._from_obj includes gravitino support."""
        import inspect

        # Check that _from_obj method includes from_gravitino
        catalog_from_obj_source = inspect.getsource(Catalog._from_obj)
        assert "from_gravitino" in catalog_from_obj_source

        table_from_obj_source = inspect.getsource(Table._from_obj)
        assert "from_gravitino" in table_from_obj_source


def test_standalone_integration():
    """Standalone test function that can be run independently."""
    print("🧪 Testing Gravitino catalog integration...\n")

    try:
        # Test basic factory methods
        client = GravitinoClient(
            endpoint="http://localhost:8090", metalake_name="metalake_demo", auth_type="simple", username="admin"
        )
        catalog = Catalog.from_gravitino(client)
        print(f"Created catalog: {catalog}")

        # Test table creation
        table_info = GravitinoTableInfo(
            name="test_table",
            catalog="test_catalog",
            schema="test_schema",
            table_type="EXTERNAL",
            storage_location="s3://test-bucket/path/",
            format="ICEBERG",
            properties={},
        )

        gravitino_table = GravitinoTable(table_info=table_info, table_uri="s3://test-bucket/path/", io_config=None)
        table = Table.from_gravitino(gravitino_table)
        print(f"Created table: {table}")

        # Test catalog methods
        has_table = catalog.has_table("nonexistent.table")
        print(f"has_table() works: {has_table}")

        tables = catalog.list_tables()
        print(f"list_tables() works: found {len(tables)} tables")

        print("\n All integration tests passed!")

    except Exception as e:
        print(f"\n Integration test failed: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    test_standalone_integration()
