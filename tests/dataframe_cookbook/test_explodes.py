import numpy as np
import pandas as pd

from daft import DataFrame, col
from daft.execution.operators import ExpressionType
from tests.conftest import assert_df_equals


def test_explode_single_col():
    data = {"explode": [[1, 2, 3], [4, 5], [], None], "repeat": ["a", "b", "c", "d"]}
    df = DataFrame.from_pydict(data)
    df = df.explode(col("explode"))
    df = df.with_column("explode_plus1", col("explode") + 1)

    assert df.schema()["explode"].daft_type == ExpressionType.python_object()

    daft_pd_df = df.to_pandas()
    pd_df = pd.DataFrame(data)
    pd_df = pd_df.explode("explode")
    pd_df["explode_plus1"] = pd_df["explode"] + 1
    pd_df = pd_df.replace({np.nan: None})
    assert_df_equals(daft_pd_df, pd_df, sort_key="explode")


def test_explode_multi_col():
    data = {
        "explode1": [[1, 2, 3], [4, 5], [], None],
        "explode2": [["a", "a", "a"], ["b", "b"], [], None],
        "repeat": ["a", "b", "c", "d"],
    }
    df = DataFrame.from_pydict(data)
    df = df.explode(col("explode1"), col("explode2"))
    df = df.with_column("explode1_plus1", col("explode1") + 1)
    df = df.with_column("explode2_startswitha", col("explode2").str.startswith("a"))

    assert df.schema()["explode1"].daft_type == ExpressionType.python_object()
    assert df.schema()["explode2"].daft_type == ExpressionType.python_object()

    daft_pd_df = df.to_pandas()
    pd_df = pd.DataFrame(data)
    pd_df = pd_df.explode(["explode1", "explode2"])
    pd_df["explode1_plus1"] = pd_df["explode1"] + 1
    pd_df["explode2_startswitha"] = pd_df["explode2"].str.startswith("a")
    pd_df = pd_df.replace({np.nan: None})
    assert_df_equals(daft_pd_df, pd_df, sort_key="explode1")
