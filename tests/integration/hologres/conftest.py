from __future__ import annotations

import os
import uuid
from collections.abc import Generator

import pytest

import daft
from daft.catalog import Catalog


def make_hologres_uri() -> str | None:
    """Build a Hologres connection URI from the environment.

    Either HOLOGRES_URI directly, or the HOLOGRES_HOST / HOLOGRES_PORT /
    HOLOGRES_USER / HOLOGRES_PASSWORD / HOLOGRES_DB combination.
    Returns None when the instance is not configured.
    """
    uri = os.environ.get("HOLOGRES_URI")
    if uri:
        return uri

    host = os.environ.get("HOLOGRES_HOST")
    if not host:
        return None

    from urllib.parse import quote

    user = os.environ.get("HOLOGRES_USER", "")
    password = os.environ.get("HOLOGRES_PASSWORD", "")
    port = os.environ.get("HOLOGRES_PORT", "80")
    db = os.environ.get("HOLOGRES_DB", "postgres")
    return f"postgresql://{quote(user)}:{quote(password)}@{host}:{port}/{db}"


def unique_table(prefix: str = "daft_it") -> str:
    """A unique lowercase table name for one test."""
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


@pytest.fixture(scope="session")
def hologres_uri() -> Generator[str, None, None]:
    """Session-scoped Hologres connection URI; skips when unreachable."""
    uri = make_hologres_uri()
    if uri is None:
        pytest.skip("Hologres instance not configured; set HOLOGRES_URI or HOLOGRES_HOST/USER/PASSWORD/DB")

    try:
        catalog = Catalog.from_hologres(uri)
        # Probe the connection with a cheap call.
        catalog.list_namespaces()
    except Exception as e:
        pytest.skip(f"Skipping Hologres tests: cannot connect ({e})")

    yield uri


@pytest.fixture(scope="session")
def hologres_catalog(hologres_uri: str) -> Catalog:
    return Catalog.from_hologres(hologres_uri)


@pytest.fixture()
def cleanup_tables(hologres_catalog: Catalog):
    """Tracks tables created during a test and drops them afterwards."""
    created: list[str] = []

    def _track(identifier: str) -> str:
        created.append(identifier)
        return identifier

    yield _track

    for identifier in created:
        try:
            hologres_catalog.drop_table(identifier)
        except Exception:
            pass


def make_test_df(rows: int = 5) -> daft.DataFrame:
    return daft.from_pydict(
        {
            "id": list(range(1, rows + 1)),
            "name": [f"row_{i}" for i in range(1, rows + 1)],
            "score": [float(i) * 1.5 for i in range(1, rows + 1)],
        }
    )
