from __future__ import annotations

import pandas as pd
import pytest

import daft

FILES = [
    "tests/assets/parquet-data/mvp.parquet",
    "tests/assets/parquet-data/parquet-with-schema-metadata.parquet",
    "tests/assets/parquet-data/sampled-tpch-with-stats.parquet",
]


@pytest.mark.parametrize("disable_task_merge", [False, True])
@pytest.mark.parametrize("path", FILES)
def test_parquet_split_by_row_groups(disable_task_merge, path):
    if disable_task_merge:
        daft.set_execution_config(
            merge_scan_tasks_min_size_bytes=0,
            merge_scan_tasks_max_size_bytes=0,
        )

    daft_df = daft.read_parquet(path)
    pd_df = pd.read_parquet(path)

    pd.testing.assert_frame_equal(pd_df, daft_df.to_pandas())
