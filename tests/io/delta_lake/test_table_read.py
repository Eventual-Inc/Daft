from __future__ import annotations

import datetime

import pandas as pd
import pytest

deltalake = pytest.importorskip("deltalake")

import daft
from daft.logical.schema import Schema


@pytest.mark.integration()
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
    pd.testing.assert_frame_equal(df.to_pandas(), pd_df)


@pytest.mark.integration()
def test_deltalake_read_full(local_deltalake_table):
    path, dfs = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    pd.testing.assert_frame_equal(df.to_pandas(), pd.concat(dfs).reset_index(drop=True))


@pytest.mark.integration()
def test_deltalake_read_show(local_deltalake_table):
    path, _ = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    df.show()
