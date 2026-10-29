from __future__ import annotations

import pytest

from daft.io._hologres._stage_sql import (
    build_copy_in_stage_sql,
    build_create_stage_sql,
    build_drop_stage_sql,
    build_insert_select_from_stage_sql_pk,
)


class TestStageAdmin:
    def test_create_stage(self):
        assert build_create_stage_sql("my_stage") == (
            "call hologres.hg_create_internal_stage('my_stage', 'default', 3600);"
        )

    def test_create_stage_custom(self):
        assert build_create_stage_sql("s", "grp", 60) == "call hologres.hg_create_internal_stage('s', 'grp', 60);"

    def test_drop_stage(self):
        assert build_drop_stage_sql("my_stage") == "call hologres.hg_drop_internal_stage('my_stage');"

    def test_copy_in(self):
        assert build_copy_in_stage_sql("stage", "file_0.arrow") == (
            "copy external_files(path='internal_stage://stage/file_0.arrow') from stdin;"
        )


COLUMNS = [
    ("id", "int4", 0, 0),
    ("name", "text", 0, 0),
    ("payload", "jsonb", 0, 0),
    ("amount", "numeric", 38, 9),
]


class TestInsertSelect:
    def test_insert_into(self):
        sql = build_insert_select_from_stage_sql_pk("public", "t", COLUMNS, [], ["stage1"])
        assert sql.startswith('insert into "public"."t" (')
        assert '"id", "name", "payload", "amount"' in sql
        assert '"payload"::jsonb' in sql
        assert "external_files(path='internal_stage://stage1')" in sql
        assert '"payload" text' in sql
        assert '"amount" decimal(38,9)' in sql
        assert "on conflict" not in sql

    def test_insert_overwrite(self):
        sql = build_insert_select_from_stage_sql_pk("public", "t", COLUMNS, [], ["stage1"], is_overwrite=True)
        assert sql.startswith("insert overwrite")
        assert "on conflict" not in sql

    def test_on_conflict_ignore(self):
        sql = build_insert_select_from_stage_sql_pk(
            "public", "t", COLUMNS, ["id"], ["stage1"], conflict_action="ignore"
        )
        assert 'on conflict ("id") do nothing' in sql

    def test_on_conflict_update(self):
        sql = build_insert_select_from_stage_sql_pk(
            "public", "t", COLUMNS, ["id"], ["stage1", "stage2"], conflict_action="update"
        )
        assert 'on conflict ("id") do update set' in sql
        assert '"id"=excluded."id"' in sql
        assert '"payload"=excluded."payload"' in sql
        assert "internal_stage://stage1,internal_stage://stage2" in sql

    def test_multiple_stages(self):
        sql = build_insert_select_from_stage_sql_pk("public", "t", COLUMNS, [], ["a", "b"])
        assert "internal_stage://a,internal_stage://b" in sql

    def test_serial_types_resolved(self):
        cols = [("id", "serial", 0, 0), ("big", "bigserial", 0, 0), ("small", "smallserial", 0, 0)]
        sql = build_insert_select_from_stage_sql_pk("public", "t", cols, [], ["s"])
        assert '"id" int' in sql
        assert '"big" bigint' in sql
        assert '"small" smallint' in sql

    def test_empty_stages_raises(self):
        # An empty stage list would produce invalid SQL; guard against it.
        with pytest.raises(ValueError, match="At least one stage"):
            build_insert_select_from_stage_sql_pk("public", "t", COLUMNS, [], [])
