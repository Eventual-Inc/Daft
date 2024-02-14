from __future__ import annotations

import pytest

deltalake = pytest.importorskip("deltalake")

import pandas as pd


@pytest.fixture
def local_deltalake_table(request, tmp_path) -> deltalake.DeltaTable:
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})
    path = tmp_path / "some_table"
    deltalake.write_deltalake(path, df)
    yield deltalake.DeltaTable(path)
