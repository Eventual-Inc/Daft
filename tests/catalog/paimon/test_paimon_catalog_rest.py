"""Unit tests for PaimonCatalog REST catalog path (using mocks, no real server needed)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from daft.catalog import Catalog, Identifier, NotFoundError

pypaimon = pytest.importorskip("pypaimon")

from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    TableNotExistException,
)

# ---------------------------------------------------------------------------
# Helpers: build a mock inner catalog that mimics RESTCatalog's interface
# ---------------------------------------------------------------------------


def _make_rest_inner(
    databases: list[str] | None = None,
    tables_by_db: dict[str, list[str]] | None = None,
):
    """Return a mock that quacks like a RESTCatalog."""
    inner = MagicMock(spec=pypaimon.catalog.catalog.Catalog)

    # REST-only methods (not on the abstract base):
    inner.list_databases = MagicMock(return_value=databases or [])
    inner.list_tables = MagicMock(side_effect=lambda db: (tables_by_db or {}).get(db, []))
    inner.drop_database = MagicMock()
    inner.drop_table = MagicMock()

    # Methods shared with FileSystemCatalog:
    inner.get_database = MagicMock()
    inner.create_database = MagicMock()
    inner.get_table = MagicMock()
    inner.create_table = MagicMock()

    return inner


# ---------------------------------------------------------------------------
# _list_namespaces — REST path
# ---------------------------------------------------------------------------


def test_rest_list_namespaces_delegates_to_list_databases():
    inner = _make_rest_inner(databases=["db_a", "db_b", "db_c"])
    cat = Catalog.from_paimon(inner)

    result = cat.list_namespaces()

    inner.list_databases.assert_called_once()
    assert Identifier("db_a") in result
    assert Identifier("db_b") in result
    assert Identifier("db_c") in result


def test_rest_list_namespaces_with_pattern():
    inner = _make_rest_inner(databases=["prod_orders", "prod_users", "staging_data"])
    cat = Catalog.from_paimon(inner)

    result = cat.list_namespaces(pattern="prod")

    assert all(str(r).startswith("prod") for r in result)
    assert len(result) == 2


def test_rest_list_namespaces_empty():
    inner = _make_rest_inner(databases=[])
    cat = Catalog.from_paimon(inner)
    assert cat.list_namespaces() == []


# ---------------------------------------------------------------------------
# _list_tables — REST path
# ---------------------------------------------------------------------------


def test_rest_list_tables_delegates_to_list_databases_and_list_tables():
    inner = _make_rest_inner(
        databases=["db_a", "db_b"],
        tables_by_db={"db_a": ["orders", "users"], "db_b": ["events"]},
    )
    cat = Catalog.from_paimon(inner)

    result = cat.list_tables()

    assert Identifier("db_a", "orders") in result
    assert Identifier("db_a", "users") in result
    assert Identifier("db_b", "events") in result
    assert len(result) == 3


def test_rest_list_tables_calls_list_tables_per_database():
    inner = _make_rest_inner(
        databases=["db_a", "db_b"],
        tables_by_db={"db_a": ["t1"], "db_b": ["t2"]},
    )
    cat = Catalog.from_paimon(inner)
    cat.list_tables()

    assert inner.list_tables.call_count == 2
    inner.list_tables.assert_any_call("db_a")
    inner.list_tables.assert_any_call("db_b")


def test_rest_list_tables_with_pattern():
    inner = _make_rest_inner(
        databases=["db_a"],
        tables_by_db={"db_a": ["orders", "order_items", "users"]},
    )
    cat = Catalog.from_paimon(inner)

    result = cat.list_tables(pattern="db_a.order")

    assert Identifier("db_a", "orders") in result
    assert Identifier("db_a", "order_items") in result
    assert Identifier("db_a", "users") not in result


def test_rest_list_tables_empty_database():
    inner = _make_rest_inner(
        databases=["empty_db"],
        tables_by_db={"empty_db": []},
    )
    cat = Catalog.from_paimon(inner)
    assert cat.list_tables() == []


# ---------------------------------------------------------------------------
# _drop_namespace — REST path
# ---------------------------------------------------------------------------


def test_rest_drop_namespace_delegates_to_drop_database():
    inner = _make_rest_inner(databases=["my_db"])
    cat = Catalog.from_paimon(inner)

    cat.drop_namespace("my_db")

    inner.drop_database.assert_called_once_with("my_db", ignore_if_not_exists=False)


def test_rest_drop_namespace_not_found_raises_notfounderror():
    inner = _make_rest_inner()
    inner.drop_database.side_effect = DatabaseNotExistException("my_db")
    cat = Catalog.from_paimon(inner)

    with pytest.raises(NotFoundError):
        cat.drop_namespace("my_db")


# ---------------------------------------------------------------------------
# _drop_table — REST path
# ---------------------------------------------------------------------------


def test_rest_drop_table_delegates_to_drop_table():
    inner = _make_rest_inner()
    cat = Catalog.from_paimon(inner)

    cat.drop_table("my_db.my_table")

    inner.drop_table.assert_called_once_with("my_db.my_table", ignore_if_not_exists=False)


def test_rest_drop_table_not_found_raises_notfounderror():
    inner = _make_rest_inner()
    fake_ident = MagicMock()
    fake_ident.get_full_name.return_value = "my_db.my_table"
    inner.drop_table.side_effect = TableNotExistException(fake_ident)
    cat = Catalog.from_paimon(inner)

    with pytest.raises(NotFoundError):
        cat.drop_table("my_db.my_table")


# ---------------------------------------------------------------------------
# _has_namespace — strips catalog prefix from multi-part identifiers
# ---------------------------------------------------------------------------


def test_has_namespace_single_part():
    inner = _make_rest_inner()
    inner.get_database.return_value = MagicMock()
    cat = Catalog.from_paimon(inner)

    assert cat.has_namespace("my_db") is True
    inner.get_database.assert_called_once_with("my_db")


def test_has_namespace_two_part_strips_catalog_prefix():
    """Session may pass a 2-part identifier (catalog, db); only the db part should be used."""
    inner = _make_rest_inner()
    inner.get_database.return_value = MagicMock()
    cat = Catalog.from_paimon(inner)

    # Manually call _has_namespace with a 2-part identifier (simulates Session behaviour)
    assert cat._has_namespace(Identifier("my_catalog", "my_db")) is True  # type: ignore[attr-defined]
    inner.get_database.assert_called_once_with("my_db")


def test_has_namespace_not_found_returns_false():
    inner = _make_rest_inner()
    inner.get_database.side_effect = DatabaseNotExistException("nope")
    cat = Catalog.from_paimon(inner)

    assert cat.has_namespace("nope") is False


# ---------------------------------------------------------------------------
# _create_namespace — strips catalog prefix
# ---------------------------------------------------------------------------


def test_create_namespace_uses_last_part_of_identifier():
    inner = _make_rest_inner()
    cat = Catalog.from_paimon(inner)

    cat._create_namespace(Identifier("catalog_prefix", "new_db"))  # type: ignore[attr-defined]

    inner.create_database.assert_called_once_with("new_db", ignore_if_exists=False)
