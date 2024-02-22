from __future__ import annotations

import datetime

import pandas as pd
import pyarrow as pa
import pytest

deltalake = pytest.importorskip("deltalake")
PYARROW_GE_13_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (13, 0, 0)


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
    if PYARROW_GE_13_0_0:
        # Delta Lake casts timestamps to microsecond resolution on ingress with later pyarrow versions, so we
        # preemptively cast the Pandas DataFrame here to make equality assertions easier later.
        base_df["e"] = base_df["e"].astype("datetime64[us]")
    dfs = []
    for part_idx in range(request.param):
        part_df = base_df.copy()
        part_df["part_idx"] = pd.Series([part_idx] * 3)
        deltalake.write_deltalake(path, part_df, mode="append", partition_by=partition_by)
        dfs.append(part_df)
    # NOTE: Delta Lake returns files in reverse-chronological order (most recently written first) from the transaction
    # log.
    yield path, list(reversed(dfs))
