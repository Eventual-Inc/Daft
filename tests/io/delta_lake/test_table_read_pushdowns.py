from __future__ import annotations

import datetime

import pandas as pd
import pytest

deltalake = pytest.importorskip("deltalake")

import daft
from daft.logical.schema import Schema


@pytest.mark.integration()
def test_daft_deltalake_read_predicate_pushdown_on_data(local_deltalake_table):
    path, dfs = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    df = df.where(df["a"] == 2)
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    pd.testing.assert_frame_equal(
        df.to_pandas(), pd.concat([pd_df[pd_df["a"] == 2] for pd_df in dfs]).reset_index(drop=True)
    )


@pytest.mark.integration()
def test_daft_deltalake_read_predicate_pushdown_on_part(local_deltalake_table):
    path, dfs = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    df = df.where(df["part_idx"] == 2)
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    pd.testing.assert_frame_equal(
        df.to_pandas(), pd.concat([pd_df[pd_df["part_idx"] == 2] for pd_df in dfs]).reset_index(drop=True)
    )


@pytest.mark.integration()
def test_daft_deltalake_read_predicate_pushdown_on_part_non_eq(local_deltalake_table):
    path, dfs = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    df = df.where(df["part_idx"] < 3)
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    pd.testing.assert_frame_equal(
        df.to_pandas(), pd.concat([pd_df[pd_df["part_idx"] < 3] for pd_df in dfs]).reset_index(drop=True)
    )


@pytest.mark.integration()
def test_daft_deltalake_read_predicate_pushdown_on_part_and_data(local_deltalake_table):
    path, dfs = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    df = df.where((df["part_idx"] == 2) & (df["e"] == datetime.datetime(2024, 2, 11)))
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    pd.testing.assert_frame_equal(
        df.to_pandas(),
        pd.concat(
            [pd_df[(pd_df["part_idx"] == 2) & (pd_df["e"] == datetime.datetime(2024, 2, 11))] for pd_df in dfs]
        ).reset_index(drop=True),
    )


@pytest.mark.integration()
def test_daft_deltalake_read_predicate_pushdown_on_part_and_data_same_clause(local_deltalake_table):
    path, dfs = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    df = df.where(df["part_idx"] < df["a"])
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    pd.testing.assert_frame_equal(
        df.to_pandas(),
        pd.concat([pd_df[pd_df["part_idx"] < pd_df["a"]] for pd_df in dfs]).reset_index(drop=True),
    )


@pytest.mark.integration()
def test_daft_deltalake_read_predicate_pushdown_on_part_empty(local_deltalake_table):
    path, dfs = local_deltalake_table
    df = daft.read_delta_lake(str(path))
    # There should only be len(dfs) - 1 partitions; see local_deltalake_table fixture.
    df = df.where(df["part_idx"] == len(dfs))
    assert df.schema() == Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    pd.testing.assert_frame_equal(
        df.to_pandas(), pd.concat([pd_df[pd_df["part_idx"] == len(dfs)] for pd_df in dfs]).reset_index(drop=True)
    )
