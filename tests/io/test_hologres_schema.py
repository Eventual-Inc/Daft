from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.io._hologres._schema_loader import (
    HologresColumn,
    _extract_precision_scale,
    _strip_type_args,
    daft_type_to_pg,
    pg_type_to_daft,
    qualified_name_for_sql,
    quote_identifier_if_needed,
)


class TestStripTypeArgs:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("character varying(255)", "character varying"),
            ("numeric(38,9)", "numeric"),
            ("text", "text"),
            ("integer[]", "integer[]"),
            ("character varying(255)[]", "character varying[]"),
            ("TIMESTAMP", "timestamp"),
        ],
    )
    def test_strip(self, raw, expected):
        assert _strip_type_args(raw) == expected


class TestExtractPrecisionScale:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("numeric(38,18)", (38, 18)),
            ("numeric(10)", (10, 0)),
            ("text", (0, 0)),
            ("numeric(x,y)", (0, 0)),
        ],
    )
    def test_extract(self, raw, expected):
        assert _extract_precision_scale(raw) == expected


class TestPgToDaft:
    @pytest.mark.parametrize(
        ("pg_type", "expected"),
        [
            ("boolean", DataType.bool()),
            ("smallint", DataType.int16()),
            ("integer", DataType.int32()),
            ("bigint", DataType.int64()),
            ("real", DataType.float32()),
            ("double precision", DataType.float64()),
            ("text", DataType.string()),
            ("character varying", DataType.string()),
            ("date", DataType.date()),
            ("timestamp", DataType.timestamp("us")),
            ("timestamptz", DataType.timestamp("us", "UTC")),
            ("bytea", DataType.binary()),
            ("jsonb", DataType.string()),
            ("json", DataType.string()),
            ("uuid", DataType.string()),
        ],
    )
    def test_scalars(self, pg_type, expected):
        assert pg_type_to_daft(pg_type) == expected

    def test_numeric_with_precision(self):
        assert pg_type_to_daft("numeric", 10, 2) == DataType.decimal128(10, 2)

    def test_numeric_default(self):
        assert pg_type_to_daft("numeric") == DataType.decimal128(38, 9)

    @pytest.mark.parametrize(
        ("pg_type", "expected"),
        [
            ("int4[]", DataType.list(DataType.int32())),
            ("float4[]", DataType.list(DataType.float32())),
            ("text[]", DataType.list(DataType.string())),
        ],
    )
    def test_arrays(self, pg_type, expected):
        assert pg_type_to_daft(pg_type) == expected

    def test_unmappable_returns_none(self):
        assert pg_type_to_daft("some_custom_type") is None
        # arrays of unmappable types are unmappable too
        assert pg_type_to_daft("some_custom_type[]") is None


class TestDaftToPg:
    @pytest.mark.parametrize(
        ("dtype", "expected"),
        [
            (DataType.int8(), "smallint"),
            (DataType.int16(), "smallint"),
            (DataType.int32(), "integer"),
            (DataType.int64(), "bigint"),
            (DataType.float32(), "real"),
            (DataType.float64(), "double precision"),
            (DataType.bool(), "boolean"),
            (DataType.string(), "text"),
            (DataType.binary(), "bytea"),
            (DataType.date(), "date"),
            (DataType.timestamp("us"), "timestamp"),
            (DataType.timestamp("us", "UTC"), "timestamptz"),
            (DataType.time("us"), "time"),
            (DataType.decimal128(10, 2), "numeric(10,2)"),
            (DataType.list(DataType.int32()), "integer[]"),
            (DataType.list(DataType.string()), "text[]"),
            (DataType.list(DataType.float32()), "real[]"),
            (DataType.embedding(DataType.float32(), 4), "real[]"),
            (DataType.struct({"a": DataType.int32()}), "jsonb"),
            (DataType.map(DataType.string(), DataType.int32()), "jsonb"),
            (DataType.tensor(DataType.float32()), "jsonb"),
            (DataType.null(), "text"),
        ],
    )
    def test_mapping(self, dtype, expected):
        assert daft_type_to_pg(dtype) == expected

    def test_nested_list_falls_back_to_jsonb(self):
        assert daft_type_to_pg(DataType.list(DataType.list(DataType.int32()))) == "jsonb"
        assert daft_type_to_pg(DataType.list(DataType.struct({"a": DataType.int32()}))) == "jsonb"


class TestHologresColumn:
    def test_column_defaults(self):
        col = HologresColumn(name="a", type_name="int4")
        assert col.nullable is True
        assert col.is_primary_key is False
        assert col.precision == 0 and col.scale == 0


class TestQualifiedNames:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("events", "events"),
            ("events_2024", "events_2024"),
            ("Events", '"Events"'),
            ("order date", '"order date"'),
            ('a"b', '"a""b"'),
        ],
    )
    def test_quote_identifier_if_needed(self, name, expected):
        assert quote_identifier_if_needed(name) == expected

    def test_qualified_name_preserves_mixed_case(self):
        assert qualified_name_for_sql("analytics", "Events") == 'analytics."Events"'
        assert qualified_name_for_sql("public", "events") == "public.events"
