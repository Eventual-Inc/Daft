from __future__ import annotations

import pytest

from daft.unity_catalog import UnityCatalog


@pytest.fixture(scope="session")
def local_unity_catalog() -> UnityCatalog:
    return UnityCatalog(endpoint="http://127.0.0.1:8080", token="not-used")
