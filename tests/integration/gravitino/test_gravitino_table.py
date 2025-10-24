"""Integration tests that exercise Catalog/Table APIs backed by Gravitino."""

from __future__ import annotations

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
