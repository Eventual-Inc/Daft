"""Regression tests for daft.read_iceberg() (non-CDC) reading tables with schema evolution history.

These scenarios are permanent pytest regression tests independent of CDC: this is the regular scan
path's own correctness, not something the CDC feature introduces. `read_iceberg_changes()`
has its own, separate compatibility-gate unit tests in test_iceberg_changelog_schema.py and
end-to-end CDC tests in test_iceberg_schema_evolution_cdc.py; those lock in "is this file
judged compatible", not "does the regular scan path apply the resulting rename/promotion/
null-fill correctly" -- the two layers of coverage must both be maintained, since either one
regressing independently (a PyIceberg compatibility-judgment change, or a Daft
cast/field-id-rewrite change) would not be caught by the other.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, ListType, LongType, MapType, NestedField, StringType, StructType

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


def _rows_by_id(table) -> list[dict]:
    return sorted(daft.read_iceberg(table).to_pylist(), key=lambda r: r["id"])


def test_top_level_add_column_null_fills_old_rows(local_catalog):
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    table = local_catalog.create_table("default.add_col", schema=schema)
    table.append(pa.table({"id": [1, 2]}))
    with table.update_schema() as upd:
        upd.add_column("data", StringType())
    table.refresh()
    table.append(pa.table({"id": [3], "data": ["c"]}, schema=pa.schema([("id", pa.int64()), ("data", pa.string())])))
    table.refresh()

    assert _rows_by_id(table) == [
        {"id": 1, "data": None},
        {"id": 2, "data": None},
        {"id": 3, "data": "c"},
    ]


def test_top_level_rename_resolves_old_rows_by_field_id(local_catalog):
    schema = Schema(
        NestedField(1, "id", LongType(), required=False), NestedField(2, "data", StringType(), required=False)
    )
    table = local_catalog.create_table("default.rename", schema=schema)
    table.append(pa.table({"id": [1], "data": ["a"]}))
    with table.update_schema() as upd:
        upd.rename_column("data", "renamed_data")
    table.refresh()
    table.append(pa.table({"id": [2], "renamed_data": ["b"]}))
    table.refresh()

    assert _rows_by_id(table) == [
        {"id": 1, "renamed_data": "a"},
        {"id": 2, "renamed_data": "b"},
    ]


def test_top_level_type_promotion_reads_old_and_new_rows(local_catalog):
    schema = Schema(
        NestedField(1, "id", LongType(), required=False), NestedField(2, "val", IntegerType(), required=False)
    )
    table = local_catalog.create_table("default.promote", schema=schema)
    table.append(pa.table({"id": [1], "val": [10]}, schema=pa.schema([("id", pa.int64()), ("val", pa.int32())])))
    with table.update_schema() as upd:
        upd.update_column("val", LongType())
    table.refresh()
    # value only representable once promoted to int64, to prove the old file is actually cast
    # up rather than truncated or reinterpreted.
    table.append(pa.table({"id": [2], "val": [2**35]}, schema=pa.schema([("id", pa.int64()), ("val", pa.int64())])))
    table.refresh()

    assert _rows_by_id(table) == [
        {"id": 1, "val": 10},
        {"id": 2, "val": 2**35},
    ]


def test_struct_child_field_added(local_catalog):
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "nested", StructType(NestedField(10, "a", LongType(), required=False)), required=False),
    )
    table = local_catalog.create_table("default.struct_add", schema=schema)
    table.append(
        pa.table(
            {"id": [1], "nested": [{"a": 1}]},
            schema=pa.schema([("id", pa.int64()), ("nested", pa.struct([("a", pa.int64())]))]),
        )
    )
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

    assert _rows_by_id(table) == [
        {"id": 1, "nested": {"a": 1, "b": None}},
        {"id": 2, "nested": {"a": 2, "b": "x"}},
    ]


def test_list_of_struct_child_field_added(local_catalog):
    elem_v1 = pa.struct([("a", pa.int64())])
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(
            2,
            "items",
            ListType(20, StructType(NestedField(10, "a", LongType(), required=False)), element_required=False),
            required=False,
        ),
    )
    table = local_catalog.create_table("default.list_struct_add", schema=schema)
    table.append(
        pa.table(
            {"id": [1], "items": [[{"a": 1}]]},
            schema=pa.schema([("id", pa.int64()), ("items", pa.list_(pa.field("element", elem_v1)))]),
        )
    )
    with table.update_schema() as upd:
        upd.union_by_name(
            Schema(
                NestedField(1, "id", LongType(), required=False),
                NestedField(
                    2,
                    "items",
                    ListType(
                        20,
                        StructType(
                            NestedField(10, "a", LongType(), required=False),
                            NestedField(11, "b", StringType(), required=False),
                        ),
                        element_required=False,
                    ),
                    required=False,
                ),
            )
        )
    table.refresh()
    elem_v2 = pa.struct([("a", pa.int64()), ("b", pa.string())])
    table.append(
        pa.table(
            {"id": [2], "items": [[{"a": 2, "b": "x"}]]},
            schema=pa.schema([("id", pa.int64()), ("items", pa.list_(pa.field("element", elem_v2)))]),
        )
    )
    table.refresh()

    assert _rows_by_id(table) == [
        {"id": 1, "items": [{"a": 1, "b": None}]},
        {"id": 2, "items": [{"a": 2, "b": "x"}]},
    ]


def test_map_string_to_struct_value_field_added(local_catalog):
    val_v1 = pa.struct([("a", pa.int64())])
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(
            2,
            "kv",
            MapType(
                20, StringType(), 21, StructType(NestedField(10, "a", LongType(), required=False)), value_required=False
            ),
            required=False,
        ),
    )
    table = local_catalog.create_table("default.map_struct_add", schema=schema)
    table.append(
        pa.table(
            {"id": [1], "kv": [[("k1", {"a": 1})]]},
            schema=pa.schema(
                [
                    ("id", pa.int64()),
                    ("kv", pa.map_(pa.field("key", pa.string(), nullable=False), pa.field("value", val_v1))),
                ]
            ),
        )
    )
    with table.update_schema() as upd:
        upd.union_by_name(
            Schema(
                NestedField(1, "id", LongType(), required=False),
                NestedField(
                    2,
                    "kv",
                    MapType(
                        20,
                        StringType(),
                        21,
                        StructType(
                            NestedField(10, "a", LongType(), required=False),
                            NestedField(11, "b", StringType(), required=False),
                        ),
                        value_required=False,
                    ),
                    required=False,
                ),
            )
        )
    table.refresh()
    val_v2 = pa.struct([("a", pa.int64()), ("b", pa.string())])
    table.append(
        pa.table(
            {"id": [2], "kv": [[("k2", {"a": 2, "b": "y"})]]},
            schema=pa.schema(
                [
                    ("id", pa.int64()),
                    ("kv", pa.map_(pa.field("key", pa.string(), nullable=False), pa.field("value", val_v2))),
                ]
            ),
        )
    )
    table.refresh()

    assert _rows_by_id(table) == [
        {"id": 1, "kv": [("k1", {"a": 1, "b": None})]},
        {"id": 2, "kv": [("k2", {"a": 2, "b": "y"})]},
    ]


def test_nested_struct_child_rename(local_catalog):
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "nested", StructType(NestedField(10, "a", LongType(), required=False)), required=False),
    )
    table = local_catalog.create_table("default.nested_rename", schema=schema)
    table.append(
        pa.table(
            {"id": [1], "nested": [{"a": 1}]},
            schema=pa.schema([("id", pa.int64()), ("nested", pa.struct([("a", pa.int64())]))]),
        )
    )
    with table.update_schema() as upd:
        upd.rename_column("nested.a", "renamed_a")
    table.refresh()
    table.append(
        pa.table(
            {"id": [2], "nested": [{"renamed_a": 2}]},
            schema=pa.schema([("id", pa.int64()), ("nested", pa.struct([("renamed_a", pa.int64())]))]),
        )
    )
    table.refresh()

    assert _rows_by_id(table) == [
        {"id": 1, "nested": {"renamed_a": 1}},
        {"id": 2, "nested": {"renamed_a": 2}},
    ]


def test_nested_struct_child_type_promotion(local_catalog):
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "nested", StructType(NestedField(10, "a", IntegerType(), required=False)), required=False),
    )
    table = local_catalog.create_table("default.nested_promote", schema=schema)
    table.append(
        pa.table(
            {"id": [1], "nested": [{"a": 1}]},
            schema=pa.schema([("id", pa.int64()), ("nested", pa.struct([("a", pa.int32())]))]),
        )
    )
    with table.update_schema() as upd:
        upd.update_column("nested.a", LongType())
    table.refresh()
    table.append(
        pa.table(
            {"id": [2], "nested": [{"a": 2**35}]},
            schema=pa.schema([("id", pa.int64()), ("nested", pa.struct([("a", pa.int64())]))]),
        )
    )
    table.refresh()

    assert _rows_by_id(table) == [
        {"id": 1, "nested": {"a": 1}},
        {"id": 2, "nested": {"a": 2**35}},
    ]


def test_multiple_stacked_schema_changes(local_catalog):
    """Add a column, then rename it and promote another column in the same later update -- the common real-world pattern of several evolutions accumulating on one table."""
    schema = Schema(
        NestedField(1, "id", LongType(), required=False), NestedField(2, "val", IntegerType(), required=False)
    )
    table = local_catalog.create_table("default.stacked", schema=schema)
    table.append(pa.table({"id": [1], "val": [10]}, schema=pa.schema([("id", pa.int64()), ("val", pa.int32())])))

    with table.update_schema() as upd:
        upd.add_column("data", StringType())
    table.refresh()
    table.append(
        pa.table(
            {"id": [2], "val": [20], "data": ["x"]},
            schema=pa.schema([("id", pa.int64()), ("val", pa.int32()), ("data", pa.string())]),
        )
    )
    table.refresh()

    with table.update_schema() as upd:
        upd.rename_column("data", "renamed_data")
        upd.update_column("val", LongType())
    table.refresh()
    table.append(
        pa.table(
            {"id": [3], "val": [2**35], "renamed_data": ["y"]},
            schema=pa.schema([("id", pa.int64()), ("val", pa.int64()), ("renamed_data", pa.string())]),
        )
    )
    table.refresh()

    assert _rows_by_id(table) == [
        {"id": 1, "val": 10, "renamed_data": None},
        {"id": 2, "val": 20, "renamed_data": "x"},
        {"id": 3, "val": 2**35, "renamed_data": "y"},
    ]
