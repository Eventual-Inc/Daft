from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pytest

from daft import DataFrame, col
from tests.conftest import assert_df_equals


@pytest.mark.parametrize("nrepartitions", [1, 5])
def test_explode_single_col_arrow(nrepartitions):
    data = {"explode": pa.array([[1, 2, 3], [4, 5], [], None]), "repeat": ["a", "b", "c", "d"]}
    df = DataFrame.from_pydict(data).repartition(nrepartitions)
    df = df.explode(col("explode"))

    df.collect()
    daft_pd_df = pd.DataFrame(df._result.to_pydict())

    pd_df = pd.DataFrame(data)
    pd_df = pd_df.explode("explode")

    pd_df["explode"] = pd_df["explode"].astype(float)
    assert_df_equals(daft_pd_df, pd_df, sort_key=["explode", "repeat"])


@pytest.mark.parametrize("nrepartitions", [1, 5])
def test_explode_multi_col(nrepartitions):
    data = {
        "explode1": [[1, 2, 3], [4, 5], [], None],
        "explode2": [["a", "a", "a"], ["b", "b"], [], None],
        "repeat": ["a", "b", "c", "d"],
    }
    df = DataFrame.from_pydict(data).repartition(nrepartitions)
    df = df.explode(col("explode1"), col("explode2"))

    df.collect()
    daft_pd_df = pd.DataFrame(df._result.to_pydict())

    pd_df = pd.DataFrame(data)
    pd_df = pd_df.explode(["explode1", "explode2"])

    pd_df["explode1"] = pd_df["explode1"].astype(float)
    assert_df_equals(daft_pd_df, pd_df, sort_key=["explode1", "repeat"])
