"""Integration tests that exercise Catalog/Table APIs backed by Gravitino."""

from __future__ import annotations

import time
import uuid

import pytest

from daft.catalog import Catalog, NotFoundError


@pytest.mark.integration()
def test_catalog_from_gravitino(local_gravitino_client, gravitino_metalake):
    catalog = Catalog.from_gravitino(local_gravitino_client)

    assert catalog is not None
    assert catalog.name == f"gravitino_{gravitino_metalake}"


@pytest.mark.integration()
def test_catalog_has_table_false(local_gravitino_client):
    catalog = Catalog.from_gravitino(local_gravitino_client)

    assert catalog.has_table("nonexistent.schema.table") is False


@pytest.mark.integration()
def test_catalog_list_tables_returns_identifiers(local_gravitino_client):
    catalog = Catalog.from_gravitino(local_gravitino_client)

    tables = catalog.list_tables()
    assert isinstance(tables, list)


@pytest.mark.integration()
def test_catalog_get_table_not_found(local_gravitino_client):
    catalog = Catalog.from_gravitino(local_gravitino_client)

    with pytest.raises(NotFoundError):
        catalog.get_table("nonexistent.schema.table")


_API_HEADERS = {
    "Accept": "application/vnd.gravitino.v1+json",
    "Content-Type": "application/json",
}


def _api_request(client, method: str, path: str, **kwargs):
    """Helper to make Gravitino REST API requests."""
    url = f"{client._endpoint.rstrip('/')}/api{path}"
    response = client._session.request(method, url, headers=_API_HEADERS, timeout=30, **kwargs)
    response.raise_for_status()
    return response.json() if response.content else {}


def _ensure_metalake(client, metalake: str):
    """Ensure metalake exists, create if it doesn't."""
    url = f"{client._endpoint.rstrip('/')}/api/metalakes/{metalake}"
    response = client._session.get(url, headers=_API_HEADERS, timeout=30)
    if response.status_code == 404:
        payload = {
            "name": metalake,
            "comment": "Daft integration metalake",
            "properties": {},
        }
        _api_request(client, "POST", "/metalakes", json=payload)
    else:
        response.raise_for_status()


def _set_catalog_in_use(client, metalake: str, catalog_name: str, in_use: bool):
    """Set the catalog's in-use property."""
    path = f"/metalakes/{metalake}/catalogs/{catalog_name}"
    try:
        payload = {"inUse": in_use}
        _api_request(client, "PATCH", path, json=payload)
    except Exception:
        pass


def _wait_for_mysql(host: str = "127.0.0.1", port: int = 3306, timeout_secs: int = 60) -> None:
    """Wait for MySQL to be ready."""
    import socket

    deadline = time.time() + timeout_secs
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))
            sock.close()
            # Give MySQL a bit more time to fully initialize
            time.sleep(2)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(3)

    raise RuntimeError(f"Failed to connect to MySQL at {host}:{port} within {timeout_secs}s") from last_error


@pytest.fixture(scope="module")
def mysql_gravitino_catalog(local_gravitino_client, gravitino_metalake):
    """Create a MySQL catalog in Gravitino and set up test schemas and tables."""
    _wait_for_mysql()
    _ensure_metalake(local_gravitino_client, gravitino_metalake)

    catalog_name = f"mysql_catalog_{uuid.uuid4().hex[:8]}"

    # Create MySQL catalog in Gravitino
    # Note: Gravitino runs in Docker, so it needs to use the Docker service name 'mysql'
    catalog_payload = {
        "name": catalog_name,
        "type": "relational",
        "provider": "jdbc-mysql",
        "comment": "MySQL catalog for Daft integration tests",
        "properties": {
            "jdbc-url": "jdbc:mysql://mysql:3306",
            "jdbc-driver": "com.mysql.cj.jdbc.Driver",
            "jdbc-user": "root",
            "jdbc-password": "root",
        },
    }
    _api_request(local_gravitino_client, "POST", f"/metalakes/{gravitino_metalake}/catalogs", json=catalog_payload)

    # Create two schemas using Gravitino API
    schema1_name = f"schema_{uuid.uuid4().hex[:8]}"
    schema2_name = f"schema_{uuid.uuid4().hex[:8]}"

    for schema_name in [schema1_name, schema2_name]:
        schema_payload = {
            "name": schema_name,
            "properties": {},
        }
        _api_request(
            local_gravitino_client,
            "POST",
            f"/metalakes/{gravitino_metalake}/catalogs/{catalog_name}/schemas",
            json=schema_payload,
        )

    # Create tables in schema1 using Gravitino API
    # Note: Using simple Gravitino types that map to MySQL types
    table1_payload = {
        "name": "users",
        "comment": "Users table",
        "columns": [
            {"name": "id", "type": "integer", "comment": "User ID", "nullable": False},
            {"name": "name", "type": "varchar(100)", "comment": "User name", "nullable": True},
            {"name": "email", "type": "varchar(255)", "comment": "User email", "nullable": True},
        ],
        "properties": {},
    }
    _api_request(
        local_gravitino_client,
        "POST",
        f"/metalakes/{gravitino_metalake}/catalogs/{catalog_name}/schemas/{schema1_name}/tables",
        json=table1_payload,
    )

    table2_payload = {
        "name": "orders",
        "comment": "Orders table",
        "columns": [
            {"name": "order_id", "type": "integer", "comment": "Order ID", "nullable": False},
            {"name": "user_id", "type": "integer", "comment": "User ID", "nullable": False},
            {"name": "amount", "type": "decimal(10,2)", "comment": "Order amount", "nullable": True},
        ],
        "properties": {},
    }
    _api_request(
        local_gravitino_client,
        "POST",
        f"/metalakes/{gravitino_metalake}/catalogs/{catalog_name}/schemas/{schema1_name}/tables",
        json=table2_payload,
    )

    # Create table in schema2 using Gravitino API
    table3_payload = {
        "name": "products",
        "comment": "Products table",
        "columns": [
            {"name": "product_id", "type": "integer", "comment": "Product ID", "nullable": False},
            {"name": "product_name", "type": "varchar(200)", "comment": "Product name", "nullable": True},
            {"name": "price", "type": "decimal(10,2)", "comment": "Product price", "nullable": True},
        ],
        "properties": {},
    }
    _api_request(
        local_gravitino_client,
        "POST",
        f"/metalakes/{gravitino_metalake}/catalogs/{catalog_name}/schemas/{schema2_name}/tables",
        json=table3_payload,
    )

    yield {
        "catalog_name": catalog_name,
        "schema1_name": schema1_name,
        "schema2_name": schema2_name,
        "tables": {
            schema1_name: ["users", "orders"],
            schema2_name: ["products"],
        },
    }

    # Cleanup
    try:
        # Delete tables using Gravitino API
        for schema_name in [schema1_name, schema2_name]:
            for table_name in ["users", "orders", "products"]:
                try:
                    _api_request(
                        local_gravitino_client,
                        "DELETE",
                        f"/metalakes/{gravitino_metalake}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}",
                    )
                except Exception:
                    pass

        # Delete schemas using Gravitino API
        for schema_name in [schema1_name, schema2_name]:
            try:
                _api_request(
                    local_gravitino_client,
                    "DELETE",
                    f"/metalakes/{gravitino_metalake}/catalogs/{catalog_name}/schemas/{schema_name}?cascade=true",
                )
            except Exception:
                pass

        # Set catalog in-use to false before deleting
        _set_catalog_in_use(local_gravitino_client, gravitino_metalake, catalog_name, False)

        # Delete catalog from Gravitino
        _api_request(local_gravitino_client, "DELETE", f"/metalakes/{gravitino_metalake}/catalogs/{catalog_name}")
    except Exception:
        pass


@pytest.mark.integration()
def test_gravitino_mysql_integration(local_gravitino_client, mysql_gravitino_catalog):
    """Test listing MySQL catalogs, schemas, and tables through Daft's Catalog API."""
    catalog = Catalog.from_gravitino(local_gravitino_client)

    # List all tables - should include our MySQL tables
    all_tables = catalog.list_tables()
    assert isinstance(all_tables, list)

    # Convert Identifier objects to strings for comparison
    all_table_strs = [str(t) for t in all_tables]

    # Build expected table identifiers
    catalog_name = mysql_gravitino_catalog["catalog_name"]
    schema1_name = mysql_gravitino_catalog["schema1_name"]
    schema2_name = mysql_gravitino_catalog["schema2_name"]

    expected_tables = [
        f"{catalog_name}.{schema1_name}.users",
        f"{catalog_name}.{schema1_name}.orders",
        f"{catalog_name}.{schema2_name}.products",
    ]

    # Check that our tables are in the list
    for expected_table in expected_tables:
        assert expected_table in all_table_strs, f"Expected table {expected_table} not found in catalog"

    # Test has_table for existing tables
    for expected_table in expected_tables:
        assert catalog.has_table(expected_table) is True, f"has_table returned False for {expected_table}"

    # Test has_table for non-existent table
    assert catalog.has_table(f"{catalog_name}.{schema1_name}.nonexistent") is False
