from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.io._hologres._ddl import (
    ColumnDef,
    build_column_defs,
    build_create_schema_sql,
    build_create_table_sql,
    build_drop_schema_sql,
    build_drop_table_sql,
    build_set_table_property_sql,
    build_table_property_statements,
    build_truncate_table_sql,
)
from daft.logical.schema import Field, Schema


def _field(name: str, dtype: DataType):
    return Field.create(name, dtype)


class TestBuildColumnDefs:
    def test_simple(self):
        schema = Schema._from_fields([_field("id", DataType.int64()), _field("name", DataType.string())])
        defs = build_column_defs(schema)
        assert [(d.name, d.pg_type, d.dimension) for d in defs] == [
            ("id", "bigint", None),
            ("name", "text", None),
        ]

    def test_embedding_gets_dimension(self):
        schema = Schema._from_fields([_field("v", DataType.embedding(DataType.float32(), 4))])
        defs = build_column_defs(schema)
        assert defs[0].pg_type == "real[]"
        assert defs[0].dimension == 4

    def test_fixed_size_float_list_gets_dimension(self):
        schema = Schema._from_fields([_field("v", DataType.fixed_size_list(DataType.float32(), 8))])
        defs = build_column_defs(schema)
        assert defs[0].pg_type == "float4[]"
        assert defs[0].dimension == 8

    def test_plain_list_has_no_dimension(self):
        schema = Schema._from_fields([_field("v", DataType.list(DataType.int32()))])
        defs = build_column_defs(schema)
        assert defs[0].pg_type == "integer[]"
        assert defs[0].dimension is None


class TestBuildCreateTableSql:
    def test_simple(self):
        cols = [ColumnDef("id", "bigint"), ColumnDef("name", "text")]
        sql = build_create_table_sql("public", "t", cols).as_string()
        assert sql == 'CREATE TABLE "public"."t" ("id" bigint, "name" text)'

    def test_no_schema(self):
        sql = build_create_table_sql(None, "t", [ColumnDef("id", "bigint")]).as_string()
        assert sql == 'CREATE TABLE "t" ("id" bigint)'

    def test_primary_key(self):
        cols = [ColumnDef("id", "bigint"), ColumnDef("name", "text")]
        sql = build_create_table_sql("public", "t", cols, primary_keys=["id"]).as_string()
        assert sql == 'CREATE TABLE "public"."t" ("id" bigint, "name" text, PRIMARY KEY ("id"))'

    def test_composite_primary_key(self):
        cols = [ColumnDef("a", "bigint"), ColumnDef("b", "text")]
        sql = build_create_table_sql("public", "t", cols, primary_keys=["a", "b"]).as_string()
        assert 'PRIMARY KEY ("a", "b")' in sql

    def test_partition_by(self):
        cols = [ColumnDef("ds", "text"), ColumnDef("v", "bigint")]
        sql = build_create_table_sql("public", "t", cols, primary_keys=["ds"], partition_by="ds").as_string()
        assert sql.endswith('PARTITION BY LIST ("ds")')

    def test_if_not_exists(self):
        sql = build_create_table_sql("public", "t", [ColumnDef("id", "bigint")], if_not_exists=True).as_string()
        assert sql == 'CREATE TABLE IF NOT EXISTS "public"."t" ("id" bigint)'

    def test_vector_dimension_check(self):
        cols = [ColumnDef("v", "real[]", dimension=4)]
        sql = build_create_table_sql("public", "t", cols).as_string()
        assert sql == 'CREATE TABLE "public"."t" ("v" real[] CHECK (array_dims("v") = \'[0:3]\'))'


class TestSetTableProperty:
    def test_table_name_is_string_literal(self):
        sql = build_set_table_property_sql("public", "t", "distribution_key", "id")
        assert sql == "CALL SET_TABLE_PROPERTY('public.t', 'distribution_key', 'id')"

    def test_invalid_key_raises(self):
        with pytest.raises(ValueError):
            build_set_table_property_sql("public", "t", "", "x")


class TestTablePropertyStatements:
    def test_simple_keys(self):
        stmts = build_table_property_statements(
            "public",
            "t",
            {
                "distribution_key": "id",
                "clustering_key": "id:name",
                "orientation": "column",
                "index": "name:text_gin",
            },
        )
        assert stmts == [
            "CALL SET_TABLE_PROPERTY('public.t', 'distribution_key', 'id')",
            "CALL SET_TABLE_PROPERTY('public.t', 'clustering_key', 'id:name')",
            "CALL SET_TABLE_PROPERTY('public.t', 'orientation', 'column')",
            "CALL SET_TABLE_PROPERTY('public.t', 'index', 'name:text_gin')",
        ]

    def test_int_values(self):
        stmts = build_table_property_statements("public", "t", {"time_to_live_in_days": 30, "shard_count": 4})
        assert stmts == [
            "CALL SET_TABLE_PROPERTY('public.t', 'time_to_live_in_seconds', '2592000')",
            "CALL SET_TABLE_PROPERTY('public.t', 'shard_count', '4')",
        ]

    def test_ttl_seconds_passthrough(self):
        stmts = build_table_property_statements("public", "t", {"time_to_live_in_seconds": 86400})
        assert stmts == ["CALL SET_TABLE_PROPERTY('public.t', 'time_to_live_in_seconds', '86400')"]

    def test_proxima_vectors_dict_json_encoded(self):
        proxima = {"v": {"algorithm": "Graph", "distance_method": "SquaredEuclidean"}}
        stmts = build_table_property_statements("public", "t", {"proxima_vectors": proxima})
        assert len(stmts) == 1
        assert '"algorithm": "Graph"' in stmts[0]

    def test_table_properties_passthrough(self):
        stmts = build_table_property_statements("public", "t", {"table_properties": {"enable_columnar_jsonb": "on"}})
        assert stmts == ["CALL SET_TABLE_PROPERTY('public.t', 'enable_columnar_jsonb', 'on')"]

    def test_table_properties_dict_value_json_encoded(self):
        stmts = build_table_property_statements("public", "t", {"table_properties": {"some_json": {"a": 1}}})
        assert stmts == ["CALL SET_TABLE_PROPERTY('public.t', 'some_json', '{\"a\": 1}')"]

    def test_table_properties_non_dict_raises(self):
        with pytest.raises(ValueError, match="table_properties"):
            build_table_property_statements("public", "t", {"table_properties": "nope"})

    def test_none_values_skipped(self):
        stmts = build_table_property_statements("public", "t", {"distribution_key": None})
        assert stmts == []


class TestOtherDdl:
    def test_truncate(self):
        assert build_truncate_table_sql("public", "t") == 'TRUNCATE TABLE "public"."t"'

    def test_drop_table(self):
        assert build_drop_table_sql("public", "t").as_string() == 'DROP TABLE "public"."t"'
        assert build_drop_table_sql("public", "t", if_exists=True).as_string() == 'DROP TABLE IF EXISTS "public"."t"'
        assert build_drop_table_sql(None, "t").as_string() == 'DROP TABLE "t"'

    def test_create_schema(self):
        assert build_create_schema_sql("s").as_string() == 'CREATE SCHEMA "s"'
        assert build_create_schema_sql("s", if_not_exists=True).as_string() == 'CREATE SCHEMA IF NOT EXISTS "s"'

    def test_drop_schema(self):
        assert build_drop_schema_sql("s").as_string() == 'DROP SCHEMA "s"'
        assert (
            build_drop_schema_sql("s", if_exists=True, cascade=True).as_string() == 'DROP SCHEMA IF EXISTS "s" CASCADE'
        )
