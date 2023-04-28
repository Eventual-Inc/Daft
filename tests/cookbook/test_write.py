from __future__ import annotations

import pytest

import daft
from tests.conftest import assert_df_equals
from tests.cookbook.assets import COOKBOOK_DATA_CSV


def test_parquet_write(tmp_path):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    pd_df = df.write_parquet(tmp_path)
    read_back_pd_df = daft.read_parquet(tmp_path.as_posix() + "/*.parquet").to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df)

    assert len(pd_df.to_pandas()) == 1


def test_parquet_write_with_partitioning(tmp_path):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    pd_df = df.write_parquet(tmp_path, partition_cols=["Borough"])

    read_back_pd_df = daft.read_parquet(tmp_path.as_posix() + "/**/*.parquet").to_pandas()
    assert_df_equals(df.exclude("Borough").to_pandas(), read_back_pd_df)

    assert len(pd_df.to_pandas()) == 5


def test_csv_write(tmp_path):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    pd_df = df.write_csv(tmp_path)

    read_back_pd_df = daft.read_csv(tmp_path.as_posix() + "/*.csv").to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df)

    assert len(pd_df.to_pandas()) == 1


@pytest.mark.skip()
def test_csv_write_with_partitioning(tmp_path):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    pd_df = df.write_csv(tmp_path, partition_cols=["Borough"]).to_pandas()
    read_back_pd_df = daft.read_csv(tmp_path.as_posix() + "/**/*.csv").to_pandas()
    assert_df_equals(df.exclude("Borough").to_pandas(), read_back_pd_df)

    assert len(pd_df.to_pandas()) == 5
