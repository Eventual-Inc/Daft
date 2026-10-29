from __future__ import annotations

import datetime
import decimal

import pytest

import daft
from daft.catalog import Identifier
from daft.datatype import DataType
from tests.integration.hologres.conftest import unique_table


def _make_df(rows: int = 5) -> daft.DataFrame:
    return daft.from_pydict(
        {
            "id": list(range(1, rows + 1)),
            "name": [f"row_{i}" for i in range(1, rows + 1)],
            "score": [float(i) * 1.5 for i in range(1, rows + 1)],
        }
    )


@pytest.fixture()
def pk_table(hologres_uri: str, cleanup_tables):
    """An empty PK table (id, name, score); yields the qualified name."""
    schema = daft.Schema._from_field_name_and_types(
        [("id", DataType.int64()), ("name", DataType.string()), ("score", DataType.float64())]
    )
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))
    daft.Catalog.from_hologres(hologres_uri).create_table(
        ident, schema, properties={"primary_key": "id", "distribution_key": "id"}
    )
    yield f"public.{ident[1]}"


@pytest.mark.integration()
def test_append_and_read_back(hologres_uri: str, pk_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(pk_table)
    table.append(_make_df(5))

    out = table.read().sort("id").to_pydict()
    assert out["id"] == [1, 2, 3, 4, 5]
    assert out["name"] == ["row_1", "row_2", "row_3", "row_4", "row_5"]
    assert out["score"] == [1.5, 3.0, 4.5, 6.0, 7.5]


@pytest.mark.integration()
def test_append_upsert_updates_rows(hologres_uri: str, pk_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(pk_table)
    table.append(_make_df(5))

    upsert = daft.from_pydict(
        {
            "id": [1, 2],
            "name": ["updated_1", "updated_2"],
            "score": [99.0, 98.0],
        }
    )
    table.append(upsert)

    out = table.read().sort("id").to_pydict()
    assert out["id"] == [1, 2, 3, 4, 5]
    assert out["name"] == ["updated_1", "updated_2", "row_3", "row_4", "row_5"]
    assert out["score"] == [99.0, 98.0, 4.5, 6.0, 7.5]


@pytest.mark.integration()
def test_append_on_conflict_ignore(hologres_uri: str, pk_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(pk_table)
    table.append(_make_df(5))

    conflict = daft.from_pydict({"id": [1], "name": ["should_be_ignored"], "score": [-1.0]})
    table.append(conflict, on_conflict="ignore")

    out = table.read().to_pydict()
    assert len(out["id"]) == 5
    assert "should_be_ignored" not in out["name"]


@pytest.mark.integration()
def test_overwrite_truncates(hologres_uri: str, pk_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(pk_table)
    table.append(_make_df(5))

    replacement = daft.from_pydict({"id": [10, 20], "name": ["a", "b"], "score": [1.0, 2.0]})
    table.overwrite(replacement)

    out = table.read().sort("id").to_pydict()
    assert out["id"] == [10, 20]
    assert out["name"] == ["a", "b"]


@pytest.mark.integration()
def test_append_without_primary_key(hologres_uri: str, cleanup_tables) -> None:
    schema = daft.Schema._from_field_name_and_types([("id", DataType.int64()), ("name", DataType.string())])
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))
    catalog = daft.Catalog.from_hologres(hologres_uri)
    catalog.create_table(ident, schema)

    table = catalog.get_table(ident)
    df = daft.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    table.append(df)

    out = table.read().sort("id").to_pydict()
    assert out["id"] == [1, 2, 3]


@pytest.mark.integration()
def test_append_bulk_load_mode(hologres_uri: str, pk_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(pk_table)
    table.append(_make_df(5), copy_mode="bulk_load", on_conflict="none")

    out = table.read().to_pydict()
    assert sorted(out["id"]) == [1, 2, 3, 4, 5]


@pytest.mark.integration()
def test_append_text_format(hologres_uri: str, pk_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(pk_table)
    table.append(_make_df(5), copy_format="text")

    out = table.read().sort("id").to_pydict()
    assert out["id"] == [1, 2, 3, 4, 5]


@pytest.mark.integration()
def test_append_nulls(hologres_uri: str, pk_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(pk_table)
    table.append(
        daft.from_pydict(
            {
                "id": [1, 2],
                "name": ["with_null", None],
                "score": [None, 2.5],
            }
        )
    )
    out = table.read().sort("id").to_pydict()
    assert out["name"] == ["with_null", None]
    assert out["score"] == [None, 2.5]


@pytest.mark.integration()
def test_append_wide_types(hologres_uri: str, cleanup_tables) -> None:
    """Round-trip the type matrix the write path supports."""
    schema = daft.Schema._from_field_name_and_types(
        [
            ("id", DataType.int64()),
            ("small", DataType.int16()),
            ("big", DataType.uint64()),
            ("f32", DataType.float32()),
            ("flag", DataType.bool()),
            ("d", DataType.date()),
            ("ts", DataType.timestamp("us")),
            ("tstz", DataType.timestamp("us", "UTC")),
            ("amount", DataType.decimal128(10, 2)),
            ("tag_list", DataType.list(DataType.string())),
            ("vec", DataType.list(DataType.float32())),
            ("payload", DataType.struct({"k": DataType.string()})),
            ("blob", DataType.binary()),
        ]
    )
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))
    catalog = daft.Catalog.from_hologres(hologres_uri)
    catalog.create_table(ident, schema, properties={"primary_key": "id"})

    df = daft.from_pydict(
        {
            "id": [1, 2],
            "small": [7, 8],
            "big": [18446744073709551615 % 1000000, 42],
            "f32": [0.5, 1.5],
            "flag": [True, False],
            "d": [datetime.date(2024, 1, 1), datetime.date(2024, 6, 30)],
            "ts": [datetime.datetime(2024, 1, 1, 12, 0), datetime.datetime(2024, 1, 2, 12, 0)],
            "tstz": [
                datetime.datetime(2024, 1, 1, 12, 0, tzinfo=datetime.timezone.utc),
                datetime.datetime(2024, 1, 2, 12, 0, tzinfo=datetime.timezone.utc),
            ],
            "amount": [decimal.Decimal("1.23"), decimal.Decimal("45.67")],
            "tag_list": [["a", "b"], None],
            "vec": [[0.25, 0.75], [1.0, 2.0]],
            "payload": [{"k": "v1"}, {"k": "v2"}],
            "blob": [b"\x01\x02", None],
        }
    )
    table = catalog.get_table(ident)
    table.append(df)

    out = table.read().sort("id").to_pydict()
    assert out["id"] == [1, 2]
    assert out["small"] == [7, 8]
    assert out["f32"] == [0.5, 1.5]
    assert out["flag"] == [True, False]
    assert out["d"] == [datetime.date(2024, 1, 1), datetime.date(2024, 6, 30)]
    assert out["ts"] == [datetime.datetime(2024, 1, 1, 12, 0), datetime.datetime(2024, 1, 2, 12, 0)]
    assert out["amount"][0] == decimal.Decimal("1.23")
    assert out["tag_list"] == [["a", "b"], None]
    assert out["vec"] == [[0.25, 0.75], [1.0, 2.0]]
    # struct -> jsonb -> string on read (mirrors the PostgreSQL integration)
    import json as _json

    assert [_json.loads(v) for v in out["payload"]] == [{"k": "v1"}, {"k": "v2"}]
    assert out["blob"] == [b"\x01\x02", None]


@pytest.mark.integration()
def test_append_large_batch(hologres_uri: str, pk_table) -> None:
    """Multi-thousand-row append exercises batching through the COPY stream."""
    n = 5000
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(pk_table)
    table.append(
        daft.from_pydict(
            {
                "id": list(range(1, n + 1)),
                "name": [f"row_{i}" for i in range(1, n + 1)],
                "score": [float(i) for i in range(1, n + 1)],
            }
        )
    )
    out = table.read().to_pydict()
    assert len(out["id"]) == n


@pytest.mark.integration()
def test_append_missing_column_fails(hologres_uri: str, pk_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(pk_table)
    df = daft.from_pydict({"id": [1], "name": ["x"], "nonexistent": [1]})
    with pytest.raises(Exception, match="not found in Hologres table"):
        table.append(df)
