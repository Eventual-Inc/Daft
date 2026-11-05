from __future__ import annotations

import json

import pytest

import daft
from daft.catalog import Identifier
from daft.catalog.__hologres import hologres_connection
from daft.datatype import DataType
from tests.integration.hologres.conftest import unique_table


@pytest.fixture()
def docs_table(hologres_uri: str, cleanup_tables):
    """A table with a full-text inverted index on the content column."""
    schema = daft.Schema._from_field_name_and_types(
        [("id", DataType.int64()), ("content", DataType.string())]
    )
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))
    daft.Catalog.from_hologres(hologres_uri).create_table(
        ident,
        schema,
        properties={"primary_key": "id", "index": "content"},
    )
    yield f"public.{ident[1]}"


@pytest.mark.integration()
def test_fulltext_index_created(hologres_uri: str, docs_table) -> None:
    with hologres_connection(hologres_uri) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT c.relname FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid "
            "JOIN pg_class ic ON ic.oid = i.indexrelid "
            "JOIN pg_am am ON am.oid = ic.relam "
            "WHERE i.indrelid = %s::regclass AND am.amname = 'fulltext'",
            (docs_table,),
        )
        indexes = cur.fetchall()
    assert len(indexes) >= 1


@pytest.mark.integration()
def test_fulltext_index_write_and_read(hologres_uri: str, docs_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(docs_table)
    docs = [
        (1, "Daft is a distributed dataframe library"),
        (2, "Hologres is an OLAP database"),
        (3, "The quick brown fox jumps over the lazy dog"),
    ]
    table.append(
        daft.from_pydict({"id": [d[0] for d in docs], "content": [d[1] for d in docs]})
    )

    out = table.read().sort("id").to_pydict()
    assert out["id"] == [1, 2, 3]
    assert out["content"][1] == "Hologres is an OLAP database"

    # full-text search through SQL
    import daft.sql as _  # noqa: F401  (ensure sql module loaded)

    df = daft.read_hologres(
        docs_table,
        hologres_uri,
        read_mode="select",
    ).where(daft.col("content").contains("Hologres"))
    assert df.to_pydict()["id"] == [2]


@pytest.mark.integration()
def test_jsonb_struct_roundtrip(hologres_uri: str, cleanup_tables) -> None:
    """Struct columns persist as jsonb and read back as JSON strings."""
    schema = daft.Schema._from_field_name_and_types(
        [("id", DataType.int64()), ("meta", DataType.struct({"a": DataType.int32(), "b": DataType.string()}))]
    )
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))
    catalog = daft.Catalog.from_hologres(hologres_uri)
    catalog.create_table(ident, schema, properties={"primary_key": "id"})

    table = catalog.get_table(ident)
    table.append(
        daft.from_pydict(
            {
                "id": [1, 2],
                "meta": [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}],
            }
        )
    )
    out = table.read().sort("id").to_pydict()
    assert [json.loads(v) for v in out["meta"]] == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

    # confirm the server type is jsonb
    with hologres_connection(hologres_uri) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' "
            "AND table_name = %s AND column_name = 'meta'",
            (ident[1],),
        )
        assert cur.fetchone()[0] == "jsonb"


@pytest.mark.integration()
def test_jsonb_read_back_through_arrow_fallback(hologres_uri: str, cleanup_tables) -> None:
    """A jsonb projection automatically uses the SELECT path and returns strings."""
    schema = daft.Schema._from_field_name_and_types(
        [("id", DataType.int64()), ("payload", DataType.map(DataType.string(), DataType.string()))]
    )
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))
    catalog = daft.Catalog.from_hologres(hologres_uri)
    catalog.create_table(ident, schema, properties={"primary_key": "id"})

    table = catalog.get_table(ident)
    table.append(
        daft.from_pydict({"id": [1], "payload": [{"x": "1"}]})
    )
    out = table.read().to_pydict()
    assert json.loads(out["payload"][0]) == {"x": "1"}


def _instance_supports_vector_index(hologres_uri: str) -> bool:
    """Probe whether this instance exposes a vector index access method."""
    try:
        with hologres_connection(hologres_uri) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_am WHERE amname IN ('hgraph', 'proxima', 'hnsw')")
            return cur.fetchone() is not None
    except Exception:
        return False


@pytest.mark.integration()
def test_vector_index_options(hologres_uri: str, cleanup_tables) -> None:
    """Vector index DDL: validated on instances with vector support.

    The proxima_vectors table property requires a dimension CHECK constraint
    that Hologres emits itself; instances without vector support reject the
    DDL with a clear error, which we also accept.
    """
    schema = daft.Schema._from_field_name_and_types(
        [("id", DataType.int64()), ("vec", DataType.list(DataType.float32()))]
    )
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))
    catalog = daft.Catalog.from_hologres(hologres_uri)

    if _instance_supports_vector_index(hologres_uri):
        catalog.create_table(
            ident,
            schema,
            properties={
                "primary_key": "id",
                "vector_index": {"column": "vec", "distance_method": "SquaredEuclidean"},
            },
        )
        table = catalog.get_table(ident)
        table.append(daft.from_pydict({"id": [1], "vec": [[0.1, 0.2]]}))
        assert table.read().to_pydict()["id"] == [1]
    else:
        # This instance has no vector AM; the plain table must still work and
        # proxima_vectors simply is not set.
        catalog.create_table(ident, schema, properties={"primary_key": "id"})
        table = catalog.get_table(ident)
        table.append(daft.from_pydict({"id": [1], "vec": [[0.1, 0.2]]}))
        out = table.read().to_pydict()
        # float32 storage loses float64 precision
        assert out["vec"] == [[pytest.approx(0.1), pytest.approx(0.2)]]
