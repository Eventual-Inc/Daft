from __future__ import annotations

import datetime

import pytest

import daft
from daft.catalog import Identifier
from tests.integration.hologres.conftest import unique_table

NUM_ROWS = 60


@pytest.fixture()
def read_table(hologres_uri: str, cleanup_tables):
    """A seeded table with a representative type mix; yields (qualified name, uri)."""
    import psycopg

    name = unique_table()
    ident = Identifier("public", name)
    cleanup_tables(str(ident))

    from urllib.parse import unquote, urlparse

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
        f'CREATE TABLE public."{name}" ('
        "id bigint NOT NULL, name text, score real, flag boolean, "
        "ts timestamp, tstz timestamptz, tags text[], amount numeric(10,2), "
        "PRIMARY KEY(id))"
    )
    with conn.cursor() as c:
        sql = f"COPY public.\"{name}\" (id, name, score, flag, ts, tstz, tags, amount) FROM STDIN WITH (FORMAT 'text', STREAM_MODE 'true')"
        with c.copy(sql) as copy:
            for i in range(NUM_ROWS):
                copy.write_row(
                    (
                        i,
                        f"row_{i}",
                        i * 1.5,
                        i % 2 == 0,
                        datetime.datetime(2024, 1, 1, 12, i % 60),
                        datetime.datetime(2024, 1, 1, 12, i % 60, tzinfo=datetime.timezone.utc),
                        [f"t{i}", "x"],
                        round(i * 1.11, 2),
                    )
                )
    conn.close()
    yield f"public.{name}", hologres_uri


def _expected_ids():
    return list(range(NUM_ROWS))


@pytest.mark.integration()
def test_read_arrow_roundtrip(read_table) -> None:
    table_name, uri = read_table
    df = daft.read_hologres(table_name, uri).sort("id")
    out = df.to_pydict()

    assert out["id"] == _expected_ids()
    assert out["name"] == [f"row_{i}" for i in range(NUM_ROWS)]
    assert out["score"] == [i * 1.5 for i in range(NUM_ROWS)]
    assert out["flag"] == [i % 2 == 0 for i in range(NUM_ROWS)]
    assert out["ts"][0] == datetime.datetime(2024, 1, 1, 12, 0)
    # timestamptz: server stores ms precision; connector pins session tz to UTC
    assert out["tstz"][0] == datetime.datetime(2024, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
    assert out["tags"][5] == ["t5", "x"]
    assert float(out["amount"][3]) == pytest.approx(3.33)


@pytest.mark.integration()
def test_read_pushdowns(read_table) -> None:
    table_name, uri = read_table
    df = daft.read_hologres(table_name, uri).where(daft.col("id") < 10).select("id", "name")
    out = df.to_pydict()
    assert sorted(out["id"]) == list(range(10))
    assert set(out.keys()) == {"id", "name"}


@pytest.mark.integration()
def test_read_limit(read_table) -> None:
    table_name, uri = read_table
    df = daft.read_hologres(table_name, uri).limit(5)
    assert len(df.to_pydict()["id"]) == 5


@pytest.mark.integration()
def test_read_select_mode_matches_arrow(read_table) -> None:
    table_name, uri = read_table
    arrow = daft.read_hologres(table_name, uri, read_mode="arrow").sort("id").to_pydict()
    select = daft.read_hologres(table_name, uri, read_mode="select").sort("id").to_pydict()
    assert arrow["id"] == select["id"]
    assert arrow["name"] == select["name"]
    assert arrow["flag"] == select["flag"]


@pytest.mark.integration()
def test_read_partition_col(read_table) -> None:
    table_name, uri = read_table
    df = (
        daft.read_hologres(table_name, uri, partition_col="id", num_partitions=4, partition_bound_strategy="min-max")
        .sort("id")
    )
    out = df.to_pydict()
    assert out["id"] == _expected_ids()


@pytest.mark.integration()
def test_table_read_uses_scan(read_table) -> None:
    table_name, uri = read_table
    catalog = daft.Catalog.from_hologres(uri)
    table = catalog.get_table(table_name)
    out = table.read().sort("id").to_pydict()
    assert out["id"] == _expected_ids()


@pytest.mark.integration()
def test_table_read_select_mode(read_table) -> None:
    table_name, uri = read_table
    catalog = daft.Catalog.from_hologres(uri)
    table = catalog.get_table(table_name)
    out = table.read(read_mode="select").sort("id").to_pydict()
    assert out["id"] == _expected_ids()
