"""End-to-end tests for daft.io.iceberg.read_iceberg_changes() against tables with schema evolution history.

These exercise the full pipeline together -- baseline schema resolution
(`daft/io/iceberg/_changelog_planning.py`), the footer compatibility gate
(`daft/io/iceberg/_changelog_schema.py`, unit-tested in isolation in
test_iceberg_changelog_schema.py), and actual data materialization plus CDC metadata
columns (`_change_type`/`_change_ordinal`/`_commit_snapshot_id`) -- against real tables built
with a real PyIceberg catalog. See test_iceberg_schema_evolution.py for the equivalent regular
(non-CDC) `daft.read_iceberg()` coverage this mirrors.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, LongType, NestedField, StringType, StructType

import daft


@pytest.fixture(scope="function")
def local_catalog(tmpdir):
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
        warehouse=f"file://{tmpdir}",
    )
    catalog.create_namespace("default")
    yield catalog
    catalog.engine.dispose()


def _rows(table, **kwargs) -> list[dict]:
    return sorted(
        daft.io.iceberg.read_iceberg_changes(table, **kwargs).to_pylist(), key=lambda r: (r["_change_ordinal"], r["id"])
    )


def test_top_level_add_column_across_schema_versions(local_catalog):
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    table = local_catalog.create_table("default.add_col", schema=schema)
    table.append(pa.table({"id": [1, 2]}))
    table.refresh()
    with table.update_schema() as upd:
        upd.add_column("data", StringType())
    table.refresh()
    table.append(pa.table({"id": [3], "data": ["c"]}, schema=pa.schema([("id", pa.int64()), ("data", pa.string())])))
    table.refresh()

    rows = _rows(table)
    assert [{k: v for k, v in r.items() if k in ("id", "data", "_change_type")} for r in rows] == [
        {"id": 1, "data": None, "_change_type": "INSERT"},
        {"id": 2, "data": None, "_change_type": "INSERT"},
        {"id": 3, "data": "c", "_change_type": "INSERT"},
    ]
    assert rows[0]["_change_ordinal"] == rows[1]["_change_ordinal"] == 0
    assert rows[2]["_change_ordinal"] == 1


def test_top_level_rename_across_schema_versions(local_catalog):
    schema = Schema(
        NestedField(1, "id", LongType(), required=False), NestedField(2, "data", StringType(), required=False)
    )
    table = local_catalog.create_table("default.rename", schema=schema)
    table.append(pa.table({"id": [1], "data": ["a"]}))
    table.refresh()
    with table.update_schema() as upd:
        upd.rename_column("data", "renamed_data")
    table.refresh()
    table.append(pa.table({"id": [2], "renamed_data": ["b"]}))
    table.refresh()

    rows = _rows(table)
    assert [{k: v for k, v in r.items() if k in ("id", "renamed_data", "_change_type")} for r in rows] == [
        {"id": 1, "renamed_data": "a", "_change_type": "INSERT"},
        {"id": 2, "renamed_data": "b", "_change_type": "INSERT"},
    ]


def test_identity_partition_source_rename_preserves_historical_values(local_catalog):
    """Historical identity-partition values must follow a source-column rename by field ID."""
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "city", StringType(), required=False),
    )
    table = local_catalog.create_table("default.partition_source_rename", schema=schema)
    with table.update_spec() as upd:
        upd.add_field("city", "identity")
    table.refresh()
    table.append(pa.table({"id": [1], "city": ["Tokyo"]}))
    table.refresh()

    with table.update_schema() as upd:
        upd.rename_column("city", "location")
    table.refresh()
    assert table.spec().fields[0].name == "city"
    assert table.schema().find_field(table.spec().fields[0].source_id).name == "location"
    table.append(pa.table({"id": [2], "location": ["Osaka"]}))
    table.refresh()

    rows = _rows(table)
    assert [(row["id"], row["location"], row["_change_type"]) for row in rows] == [
        (1, "Tokyo", "INSERT"),
        (2, "Osaka", "INSERT"),
    ]


def test_top_level_type_promotion_across_schema_versions(local_catalog):
    schema = Schema(
        NestedField(1, "id", LongType(), required=False), NestedField(2, "val", IntegerType(), required=False)
    )
    table = local_catalog.create_table("default.promote", schema=schema)
    table.append(pa.table({"id": [1], "val": [10]}, schema=pa.schema([("id", pa.int64()), ("val", pa.int32())])))
    table.refresh()
    with table.update_schema() as upd:
        upd.update_column("val", LongType())
    table.refresh()
    # value only representable once promoted to int64, to prove the old file is actually cast
    # up rather than truncated or reinterpreted.
    table.append(pa.table({"id": [2], "val": [2**35]}, schema=pa.schema([("id", pa.int64()), ("val", pa.int64())])))
    table.refresh()

    rows = _rows(table)
    assert [{k: v for k, v in r.items() if k in ("id", "val", "_change_type")} for r in rows] == [
        {"id": 1, "val": 10, "_change_type": "INSERT"},
        {"id": 2, "val": 2**35, "_change_type": "INSERT"},
    ]


def test_nested_struct_child_field_added_across_schema_versions(local_catalog):
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "nested", StructType(NestedField(10, "a", LongType(), required=False)), required=False),
    )
    table = local_catalog.create_table("default.nested_add", schema=schema)
    table.append(
        pa.table(
            {"id": [1], "nested": [{"a": 1}]},
            schema=pa.schema([("id", pa.int64()), ("nested", pa.struct([("a", pa.int64())]))]),
        )
    )
    table.refresh()
    with table.update_schema() as upd:
        upd.union_by_name(
            Schema(
                NestedField(1, "id", LongType(), required=False),
                NestedField(
                    2,
                    "nested",
                    StructType(
                        NestedField(10, "a", LongType(), required=False),
                        NestedField(11, "b", StringType(), required=False),
                    ),
                    required=False,
                ),
            )
        )
    table.refresh()
    table.append(
        pa.table(
            {"id": [2], "nested": [{"a": 2, "b": "x"}]},
            schema=pa.schema([("id", pa.int64()), ("nested", pa.struct([("a", pa.int64()), ("b", pa.string())]))]),
        )
    )
    table.refresh()

    rows = _rows(table)
    assert [{k: v for k, v in r.items() if k in ("id", "nested", "_change_type")} for r in rows] == [
        {"id": 1, "nested": {"a": 1, "b": None}, "_change_type": "INSERT"},
        {"id": 2, "nested": {"a": 2, "b": "x"}, "_change_type": "INSERT"},
    ]


def test_delete_of_row_from_pre_evolution_file(local_catalog):
    """A DELETE change referencing a data file written under an earlier schema must still resolve against the current baseline, not the schema the deleted file was originally written with."""
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    table = local_catalog.create_table("default.delete_old_schema", schema=schema)
    table.append(pa.table({"id": [1, 2]}))
    table.refresh()
    with table.update_schema() as upd:
        upd.add_column("data", StringType())
    table.refresh()
    table.append(pa.table({"id": [3], "data": ["c"]}, schema=pa.schema([("id", pa.int64()), ("data", pa.string())])))
    table.refresh()
    table.delete("id = 1")  # removes a row from the pre-evolution file
    table.refresh()

    rows = _rows(table)
    delete_rows = [r for r in rows if r["_change_type"] == "DELETE"]
    assert len(delete_rows) == 1
    assert delete_rows[0]["id"] == 1
    assert delete_rows[0]["data"] is None


def test_partial_range_end_snapshot_before_evolution_uses_older_schema(local_catalog):
    """Reading a range that ends before a later schema change must reflect that earlier snapshot's own schema_id, not the table's current/latest schema."""
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    table = local_catalog.create_table("default.partial_range", schema=schema)
    table.append(pa.table({"id": [1]}))
    table.refresh()
    pre_evolution_snapshot_id = table.current_snapshot().snapshot_id

    with table.update_schema() as upd:
        upd.add_column("data", StringType())
    table.refresh()
    table.append(pa.table({"id": [2], "data": ["b"]}, schema=pa.schema([("id", pa.int64()), ("data", pa.string())])))
    table.refresh()

    df = daft.io.iceberg.read_iceberg_changes(table, end_snapshot_id=pre_evolution_snapshot_id)
    rows = sorted(df.to_pylist(), key=lambda r: r["id"])
    assert "data" not in rows[0]
    assert rows == [
        {"id": 1, "_change_type": "INSERT", "_change_ordinal": 0, "_commit_snapshot_id": pre_evolution_snapshot_id}
    ]
