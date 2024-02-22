from __future__ import annotations

import datetime
import os

import pandas as pd
import pytest

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa

import daft
from daft.logical.schema import Schema

PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (8, 0, 0)
micropartitions_disabled = os.getenv("DAFT_MICROPARTITIONS", "1") != "1"
pytestmark = pytest.mark.skipif(
    PYARROW_LE_8_0_0 or micropartitions_disabled,
    reason="deltalake only supported if pyarrow >= 8.0.0 and micropartitions are enabled",
)
PYARROW_GE_13_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (13, 0, 0)


def test_deltalake_read_basic(tmp_path):
    pd_df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["foo", "bar", "baz"],
            "c": [datetime.datetime(2024, 2, 10), datetime.datetime(2024, 2, 11), datetime.datetime(2024, 2, 12)],
        }
    )
    path = tmp_path / "some_table"
    deltalake.write_deltalake(path, pd_df)
    df = daft.read_delta_lake(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    if PYARROW_GE_13_0_0:
        # Delta Lake casts timestamps to microsecond resolution on ingress with later pyarrow versions.
        pd_df["c"] = pd_df["c"].astype("datetime64[us]")
    pd.testing.assert_frame_equal(df.to_pandas(), pd_df)


def test_deltalake_read_full(local_deltalake_table):
    path, dfs = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    pd.testing.assert_frame_equal(df.to_pandas(), pd.concat(dfs).reset_index(drop=True))


def test_deltalake_read_show(local_deltalake_table):
    path, _ = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    df.show()
