"""Generic-path compatibility: daft.read_sql / DataFrame.write_sql on Hologres.

These tests validate that the PostgreSQL-generic Daft APIs work against a
Hologres instance without any Hologres-specific code.
"""

from __future__ import annotations

import datetime
import decimal

import pytest

import daft
from daft.datatype import DataType
from tests.integration.hologres.conftest import unique_table


@pytest.fixture()
def compat_table(hologres_uri: str, cleanup_tables):
    """A seeded table created through plain SQL, outside Daft."""
    from urllib.parse import unquote, urlparse

    import psycopg

    name = unique_table()
    cleanup_tables(f"public.{name}")
    parsed = urlparse(hologres_uri)
    conn = psycopg.connect(
        host=parsed.hostname,
        port=parsed.port or 80,
        dbname=parsed.path.lstrip("/"),
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        autocommit=True,
    )
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS public." + name)
    cur.execute(
        f'CREATE TABLE public."{name}" (id bigint NOT NULL, name text, score double precision, '
        "flag boolean, ts timestamp, amount numeric(10,2), PRIMARY KEY(id))"
    )
    with conn.cursor() as c:
        q = (
            f'COPY public."{name}" (id, name, score, flag, ts, amount) FROM STDIN '
            "WITH (FORMAT 'text', STREAM_MODE 'true')"
        )
        with c.copy(q) as copy:
            for i in range(1, 21):
                copy.write_row(
                    (
                        i,
                        f"row_{i}",
                        i * 1.5,
                        i % 2 == 0,
                        datetime.datetime(2024, 1, 1, 12, i % 60),
                        decimal.Decimal(f"{i}.50"),
                    )
                )
    conn.close()
    yield f"public.{name}"


@pytest.mark.integration()
def test_generic_read_sql_connectorx(hologres_uri: str, compat_table) -> None:
    df = daft.read_sql(f'SELECT id, name, score FROM {compat_table} WHERE id <= 10', hologres_uri)
    out = df.sort("id").to_pydict()
    assert out["id"] == list(range(1, 11))
    assert out["score"] == [i * 1.5 for i in range(1, 11)]


@pytest.mark.integration()
def test_generic_read_sql_sqlalchemy(hologres_uri: str, compat_table) -> None:
    # pin the SQLAlchemy+psycopg driver explicitly
    driver_uri = hologres_uri.replace("postgresql://", "postgresql+psycopg://")
    df = daft.read_sql(f'SELECT id, name FROM {compat_table}', driver_uri)
    out = df.sort("id").to_pydict()
    assert out["id"] == list(range(1, 21))


@pytest.mark.integration()
def test_generic_write_sql(hologres_uri: str, cleanup_tables) -> None:

    name = unique_table()
    cleanup_tables(f"public.{name}")
    driver_uri = hologres_uri.replace("postgresql://", "postgresql+psycopg://")

    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
        }
    )
    # NOTE: pandas to_sql (the generic path) cannot handle schema-qualified
    # table names, so writes go to the default schema (public).
    df.write_sql(name, driver_uri, write_mode="fail")

    df2 = daft.read_sql(f'SELECT id, name FROM public."{name}"', hologres_uri)
    out = df2.sort("id").to_pydict()
    assert out["id"] == [1, 2, 3]
    assert out["name"] == ["a", "b", "c"]


@pytest.mark.integration()
def test_generic_write_sql_schema_inference(hologres_uri: str, cleanup_tables) -> None:
    """write_sql creates the table with SQL types readable back by read_hologres."""
    import sqlalchemy

    name = unique_table()
    cleanup_tables(f"public.{name}")
    driver_uri = hologres_uri.replace("postgresql://", "postgresql+psycopg://")

    df = daft.from_pydict(
        {
            "id": [1, 2],
            "ts": [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 2)],
        }
    )
    df.write_sql(name, driver_uri, write_mode="fail", column_types={"id": sqlalchemy.Integer})

    catalog = daft.Catalog.from_hologres(hologres_uri)
    schema = catalog.get_table(f"public.{name}").schema()
    # sqlalchemy.Integer maps to PG INTEGER (int32)
    assert schema["id"].dtype == DataType.int32()
    assert schema["ts"].dtype == DataType.timestamp("us")
