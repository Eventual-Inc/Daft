from __future__ import annotations

import pytest

import daft
from daft.catalog import Catalog, Identifier
from daft.datatype import DataType
from tests.integration.hologres.conftest import unique_table


def _create_table(catalog: Catalog, ident: Identifier, schema, properties=None):
    catalog.create_table(ident, schema, properties=properties)


@pytest.mark.integration()
def test_table_schema_roundtrip(hologres_catalog: Catalog, cleanup_tables) -> None:
    schema = daft.Schema._from_field_name_and_types(
        [
            ("id", DataType.int64()),
            ("name", DataType.string()),
            ("score", DataType.float32()),
            ("payload", DataType.struct({"k": DataType.string()})),  # -> jsonb -> string
            ("tags", DataType.list(DataType.string())),
        ]
    )
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))

    _create_table(hologres_catalog, ident, schema, properties={"primary_key": "id"})

    table = hologres_catalog.get_table(ident)
    read_back = table.schema()

    assert read_back["id"].dtype == DataType.int64()
    assert read_back["name"].dtype == DataType.string()
    assert read_back["score"].dtype == DataType.float32()
    # jsonb reads back as string (mirrors the PostgreSQL integration)
    assert read_back["payload"].dtype == DataType.string()
    assert read_back["tags"].dtype == DataType.list(DataType.string())
    assert list(read_back.column_names()) == ["id", "name", "score", "payload", "tags"]


@pytest.mark.integration()
def test_table_metadata(hologres_catalog: Catalog, cleanup_tables) -> None:
    schema = daft.Schema._from_field_name_and_types([("id", DataType.int64()), ("name", DataType.string())])
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))

    _create_table(
        hologres_catalog,
        ident,
        schema,
        properties={"primary_key": "id", "distribution_key": "id"},
    )

    table = hologres_catalog.get_table(ident)
    holo = table._load_hologres_schema()

    assert holo.schema_name == "public"
    assert holo.table_name == ident[1]
    assert holo.primary_keys == ["id"]
    assert holo.has_primary_key
    assert holo.distribution_keys == ["id"]
    # shard_count is a table-group-level setting; it must still be discoverable
    assert holo.shard_count > 0
    assert holo.partition_column is None
    assert holo.is_partition_parent is False


@pytest.mark.integration()
def test_table_metadata_partitioned(hologres_catalog: Catalog, cleanup_tables) -> None:
    schema = daft.Schema._from_field_name_and_types([("ds", DataType.string()), ("id", DataType.int64())])
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))

    _create_table(
        hologres_catalog,
        ident,
        schema,
        properties={"primary_key": "id", "partition_by": "ds"},
    )

    table = hologres_catalog.get_table(ident)
    holo = table._load_hologres_schema()

    # Hologres auto-includes the partition column in the primary key.
    assert holo.partition_column == "ds"
    assert holo.is_partition_parent
    assert "ds" in holo.primary_keys

    # The catalog must not list the auto-created partition children.
    tables = hologres_catalog.list_tables()
    table_names = [t[1] for t in tables if t[0] == "public"]
    assert table_names.count(ident[1]) == 1


@pytest.mark.integration()
def test_table_schema_not_found(hologres_uri: str) -> None:
    from daft.catalog import NotFoundError
    from daft.catalog.__hologres import HologresTable

    # Construct directly to bypass the catalog's has_table pre-check.
    table = HologresTable._from_catalog(hologres_uri, Identifier("public", unique_table("nope")))
    with pytest.raises(NotFoundError):
        table._load_hologres_schema()
