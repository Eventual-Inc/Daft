from __future__ import annotations

import datetime

import pandas as pd
import pytest

deltalake = pytest.importorskip("deltalake")

import daft
from daft.logical.schema import Schema


@pytest.fixture(params=[1, 2, 10])
def local_deltalake_table(request, tmp_path) -> deltalake.DeltaTable:
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
        deltalake.write_deltalake(path, part_df, mode="append")
        dfs.append(part_df)
    # NOTE: Delta Lake returns files in reverse-chronological order (most recently written first) from the transaction
    # log.
    yield path, list(reversed(dfs))


@pytest.mark.integration()
def test_daft_deltalake_read_basic(tmp_path):
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
def test_daft_deltalake_read_full(local_deltalake_table):
    path, dfs = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    pd.testing.assert_frame_equal(df.to_pandas(), pd.concat(dfs).reset_index(drop=True))


@pytest.mark.integration()
def test_daft_deltalake_read_show(local_deltalake_table):
    path, _ = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    df.show()


@pytest.mark.integration()
def test_daft_deltalake_read_predicate_pushdown(local_deltalake_table):
    path, dfs = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    df = df.where(df["a"] == 2)
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    pd.testing.assert_frame_equal(
        df.to_pandas(), pd.concat([pd_df[pd_df["a"] == 2] for pd_df in dfs]).reset_index(drop=True)
    )
