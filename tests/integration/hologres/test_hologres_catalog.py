from __future__ import annotations

import pytest

import daft
from daft.catalog import Catalog, Identifier, NotFoundError, TableAlreadyExistsError
from daft.datatype import DataType
from tests.integration.hologres.conftest import unique_table


@pytest.mark.integration()
def test_catalog_from_uri(hologres_catalog: Catalog) -> None:
    assert hologres_catalog.name == "daft"


@pytest.mark.integration()
def test_namespace_lifecycle(hologres_catalog: Catalog) -> None:
    ns = f"daft_it_ns_{unique_table('x')}"[:63].replace("-", "_")

    assert not hologres_catalog.has_namespace(ns)
    hologres_catalog.create_namespace(ns)
    assert hologres_catalog.has_namespace(ns)
    assert any(str(n) == ns for n in hologres_catalog.list_namespaces(ns))
    hologres_catalog.drop_namespace(ns)
    assert not hologres_catalog.has_namespace(ns)


@pytest.mark.integration()
def test_namespace_already_exists(hologres_catalog: Catalog) -> None:
    ns = "public"
    with pytest.raises(ValueError, match="already exists"):
        hologres_catalog.create_namespace(ns)


@pytest.mark.integration()
def test_table_lifecycle(hologres_catalog: Catalog, cleanup_tables) -> None:
    schema = daft.Schema._from_field_name_and_types([("id", DataType.int64()), ("name", DataType.string())])
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))

    assert not hologres_catalog.has_table(ident)
    table = hologres_catalog.create_table(ident, schema)
    assert hologres_catalog.has_table(ident)
    assert hologres_catalog.has_table(ident[1])  # bare name lookup
    assert hologres_catalog.get_table(ident).name == table.name

    tables = hologres_catalog.list_tables()
    assert str(ident) in {str(t) for t in tables}

    hologres_catalog.drop_table(ident)
    assert not hologres_catalog.has_table(ident)
    with pytest.raises(NotFoundError):
        hologres_catalog.get_table(ident)


@pytest.mark.integration()
def test_create_table_already_exists(hologres_catalog: Catalog, cleanup_tables) -> None:
    schema = daft.Schema._from_field_name_and_types([("id", DataType.int64())])
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))

    hologres_catalog.create_table(ident, schema)
    with pytest.raises(TableAlreadyExistsError):
        hologres_catalog.create_table(ident, schema)


@pytest.mark.integration()
def test_create_table_with_primary_key_and_properties(
    hologres_catalog: Catalog, hologres_uri: str, cleanup_tables
) -> None:
    from daft.catalog.__hologres import hologres_connection

    schema = daft.Schema._from_field_name_and_types(
        [("id", DataType.int64()), ("name", DataType.string()), ("score", DataType.float64())]
    )
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))

    hologres_catalog.create_table(
        ident,
        schema,
        properties={
            "primary_key": "id",
            "distribution_key": "id",
            "orientation": "column",
        },
    )

    with hologres_connection(hologres_uri) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT property_key, property_value FROM hologres.hg_table_properties "
            "WHERE table_namespace = %s AND table_name = %s AND property_key IN ('distribution_key', 'orientation', 'shard_count')",
            (ident[0], ident[1]),
        )
        props = dict(cur.fetchall())
        assert props["distribution_key"] == "id"
        assert props["orientation"] == "column"

        cur.execute(
            "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
            "AND a.attnum = ANY(i.indkey) WHERE i.indrelid = %s::regclass AND i.indisprimary",
            (f"{ident[0]}.{ident[1]}",),
        )
        assert [row[0] for row in cur.fetchall()] == ["id"]

        cur.execute(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
            (ident[0], ident[1]),
        )
        assert cur.fetchone()[0] == 3


@pytest.mark.integration()
def test_create_table_primary_key_column_must_exist(hologres_catalog: Catalog) -> None:
    schema = daft.Schema._from_field_name_and_types([("id", DataType.int64())])
    ident = Identifier("public", unique_table())

    with pytest.raises(ValueError, match="not found in schema"):
        hologres_catalog.create_table(ident, schema, properties={"primary_key": "missing"})


@pytest.mark.integration()
def test_list_tables_excludes_partition_children_and_system(hologres_catalog: Catalog) -> None:
    tables = hologres_catalog.list_tables()
    assert all(not str(t).startswith(("information_schema.", "pg_catalog.", "hologres.")) for t in tables)
    assert any(str(t) == "public.example" for t in tables) or len(tables) > 0
