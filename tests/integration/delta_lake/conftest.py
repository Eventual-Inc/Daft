from __future__ import annotations

import datetime

import pandas as pd
import pytest

deltalake = pytest.importorskip("deltalake")


@pytest.fixture(params=[None, "part_idx"])
def partition_by(request) -> str | None:
    yield request.param


@pytest.fixture(params=[1, 2, 10])
def local_deltalake_table(request, tmp_path, partition_by) -> deltalake.DeltaTable:
    path = tmp_path / "some_table"
    base_df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [1.0, 2.0, 3.0],
            "c": ["foo", "bar", "baz"],
            "d": [True, False, True],
            "e": [datetime.datetime(2024, 2, 10), datetime.datetime(2024, 2, 11), datetime.datetime(2024, 2, 12)],
        }
    )
    dfs = []
    for part_idx in range(request.param):
        part_df = base_df.copy()
        part_df["part_idx"] = pd.Series([part_idx] * 3)
        deltalake.write_deltalake(path, part_df, mode="append", partition_by=partition_by)
        dfs.append(part_df)
    # NOTE: Delta Lake returns files in reverse-chronological order (most recently written first) from the transaction
    # log.
    yield path, list(reversed(dfs))
