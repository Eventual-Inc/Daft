import pandas as pd

from daft import DataFrame, col
from tests.conftest import assert_df_equals


def test_explode_single_col():
    data = {"explode": [[1, 2, 3], [4, 5, 6]], "repeat": ["a", "b"]}
    df = DataFrame.from_pydict(data)
    df = df.explode(col("explode"))

    # assert df.schema()["explode"].daft_type == ExpressionType.python_object()

    daft_pd_df = df.to_pandas()
    pd_df = pd.DataFrame(data)
    pd_df = pd_df.explode("explode")
    pd_df["explode"] = pd_df["explode"].astype(int)
    assert_df_equals(daft_pd_df, pd_df, sort_key="explode")
