from __future__ import annotations

import pytest

import daft
from daft.catalog import Identifier
from daft.catalog.__hologres import hologres_connection
from daft.datatype import DataType
from tests.integration.hologres.conftest import unique_table


@pytest.fixture()
def stage_table(hologres_uri: str, cleanup_tables):
    schema = daft.Schema._from_field_name_and_types(
        [("id", DataType.int64()), ("name", DataType.string()), ("score", DataType.float64())]
    )
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))
    daft.Catalog.from_hologres(hologres_uri).create_table(
        ident, schema, properties={"primary_key": "id", "distribution_key": "id"}
    )
    yield f"public.{ident[1]}"


def _df(ids: list[int]) -> daft.DataFrame:
    return daft.from_pydict(
        {
            "id": ids,
            "name": [f"row_{i}" for i in ids],
            "score": [float(i) for i in ids],
        }
    )


@pytest.mark.integration()
def test_stage_append_roundtrip(hologres_uri: str, stage_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(stage_table)
    table.append(_df([1, 2, 3, 4, 5]), copy_mode="stage")

    out = table.read().sort("id").to_pydict()
    assert out["id"] == [1, 2, 3, 4, 5]
    assert out["name"] == ["row_1", "row_2", "row_3", "row_4", "row_5"]


@pytest.mark.integration()
def test_stage_append_upsert(hologres_uri: str, stage_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(stage_table)
    table.append(_df([1, 2, 3]), copy_mode="stage")
    table.append(_df([2, 4]), copy_mode="stage")

    out = table.read().sort("id").to_pydict()
    assert out["id"] == [1, 2, 3, 4]
    assert out["name"] == ["row_1", "row_2", "row_3", "row_4"]


@pytest.mark.integration()
def test_stage_overwrite(hologres_uri: str, stage_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(stage_table)
    table.append(_df([1, 2, 3]), copy_mode="stage")

    table.overwrite(_df([7, 8, 9]), copy_mode="stage")

    out = table.read().sort("id").to_pydict()
    assert out["id"] == [7, 8, 9]


@pytest.mark.integration()
def test_stage_large_batch(hologres_uri: str, stage_table) -> None:
    """A batch above the 64MB stage-file threshold is split into multiple files."""
    n = 30000
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(stage_table)
    table.append(_df(list(range(1, n + 1))), copy_mode="stage")

    out = table.read().to_pydict()
    assert len(out["id"]) == n


@pytest.mark.integration()
def test_stage_mode_requires_recent_instance(hologres_uri: str, stage_table, monkeypatch) -> None:
    """Version gating produces a clear error instead of a server failure."""
    from unittest import mock

    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(stage_table)

    real_connect = None

    class FakeCursor:
        def execute(self, *a, **k):
            pass

        def fetchone(self):
            return ("Hologres 3.0.0 (build)",)

        def fetchall(self):
            return []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    with mock.patch("psycopg.connect", side_effect=lambda *a, **k: FakeConn()):
        with pytest.raises(ValueError, match="requires Hologres >= 4.1.0"):
            table.append(_df([1]), copy_mode="stage")


@pytest.mark.integration()
def test_stage_files_cleaned_up(hologres_uri: str, stage_table) -> None:
    """The internal stage is dropped after a successful commit."""
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(stage_table)
    table.append(_df([1]), copy_mode="stage")

    with hologres_connection(hologres_uri) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM hologres.hg_internal_stages WHERE stage_name LIKE %s",
            ("daft_public_%",),
        )
        # the per-write stage was dropped; other suites' stages (if any) also commit+drop
        remaining = cur.fetchone()[0]
    assert remaining == 0
