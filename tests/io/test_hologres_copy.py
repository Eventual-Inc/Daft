from __future__ import annotations

import pytest

from daft.io._hologres._copy import build_copy_sql, encode_binary_header
from daft.io._hologres.types import CopyFormat, CopyMode, OnConflictAction


class TestBuildCopySql:
    def test_stream_text_no_pk(self):
        sql = build_copy_sql("public", "t", ["a", "b"], CopyFormat.TEXT, CopyMode.STREAM, has_pk=False)
        assert sql.as_string() == (
            'COPY "public"."t"("a", "b") FROM STDIN WITH (FORMAT \'text\', STREAM_MODE \'true\')'
        )

    def test_stream_text_pk_update(self):
        sql = build_copy_sql(
            "public", "t", ["a", "b"], CopyFormat.TEXT, CopyMode.STREAM, OnConflictAction.INSERT_OR_UPDATE, has_pk=True
        )
        assert sql.as_string() == (
            'COPY "public"."t"("a", "b") FROM STDIN WITH '
            "(FORMAT 'text', STREAM_MODE 'true', ON_CONFLICT 'update')"
        )

    def test_stream_text_pk_ignore(self):
        sql = build_copy_sql(
            "public", "t", ["a"], CopyFormat.TEXT, CopyMode.STREAM, OnConflictAction.INSERT_OR_IGNORE, has_pk=True
        )
        assert "'ignore'" in sql.as_string()

    def test_stream_text_pk_none_conflict_omits_clause(self):
        sql = build_copy_sql("public", "t", ["a"], CopyFormat.TEXT, CopyMode.STREAM, OnConflictAction.NONE, has_pk=True)
        assert "ON_CONFLICT" not in sql.as_string()

    def test_bulk_load_binary(self):
        sql = build_copy_sql("s", "t", ["a"], CopyFormat.BINARY, CopyMode.BULK_LOAD, has_pk=False)
        assert sql.as_string() == 'COPY "s"."t"("a") FROM STDIN WITH (FORMAT \'binary\', STREAM_MODE \'false\')'

    def test_bulk_load_pk_conflict_requires_opt_in(self):
        # bulk_load without an explicit conflict action never emits ON_CONFLICT
        sql = build_copy_sql("s", "t", ["a"], CopyFormat.BINARY, CopyMode.BULK_LOAD, OnConflictAction.NONE, has_pk=True)
        assert "ON_CONFLICT" not in sql.as_string()

    def test_bulk_load_pk_conflict_emits_update(self):
        sql = build_copy_sql(
            "s", "t", ["a"], CopyFormat.BINARY, CopyMode.BULK_LOAD, OnConflictAction.INSERT_OR_UPDATE, has_pk=True
        )
        assert "ON_CONFLICT 'update'" in sql.as_string()

    def test_identifiers_are_quoted(self):
        sql = build_copy_sql("my schema", "my table", ["col a"], CopyFormat.TEXT, CopyMode.STREAM)
        assert '"my schema"."my table"("col a")' in sql.as_string()


class TestBinaryHeader:
    def test_header_bytes(self):
        header = encode_binary_header()
        assert header.startswith(b"PGCOPY\n\xff\r\n\x00")
        assert len(header) == 19  # signature(11) + flags(4) + extension length(4)

    @pytest.mark.parametrize(
        "fmt,mode",
        [
            (CopyFormat.TEXT, CopyMode.STREAM),
            (CopyFormat.BINARY, CopyMode.STREAM),
            (CopyFormat.BINARY, CopyMode.BULK_LOAD),
        ],
    )
    def test_enums_round_trip(self, fmt, mode):
        assert CopyFormat(fmt.value) is fmt
        assert CopyMode(mode.value) is mode
