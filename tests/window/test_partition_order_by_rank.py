from __future__ import annotations

import pandas as pd
import pytest

from daft import Window, col
from tests.conftest import assert_df_equals, get_tests_daft_runner_name


@pytest.mark.skip(reason="Window tests not yet implemented")
@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_rank_function(make_df):
    df = make_df(
        {"category": ["A", "A", "A", "B", "B", "B", "C", "C"], "sales": [100, 200, 50, 500, 100, 300, 250, 150]}
    )

    window_spec = Window().partition_by("category").order_by("sales", ascending=False)

    result = df.select(
        col("category"), col("sales"), col("sales").rank().over(window_spec).alias("rank_sales")
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 100, 300, 250, 150],
        "rank_sales": [2, 1, 3, 1, 3, 2, 1, 2],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()))


@pytest.mark.skip(reason="Dense rank function not yet implemented")
@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_dense_rank_function(make_df):
    df = make_df(
        {
            "category": ["A", "A", "A", "B", "B", "B", "B", "C", "C"],
            "sales": [100, 200, 50, 500, 100, 100, 300, 250, 150],
        }
    )

    window_spec = Window().partition_by("category").order_by("sales", ascending=False)

    result = df.select(
        col("category"), col("sales"), col("sales").dense_rank().over(window_spec).alias("dense_rank_sales")
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 100, 100, 300, 250, 150],
        "dense_rank_sales": [2, 1, 3, 1, 3, 3, 2, 1, 2],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()))
