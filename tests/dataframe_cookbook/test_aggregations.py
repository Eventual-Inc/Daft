import pandas as pd
import pytest

from daft.expressions import col
from tests.dataframe_cookbook.conftest import assert_df_equals

COL_SUBSET = ["Complaint Type", "Borough", "Descriptor"]


@pytest.mark.tdd_all
def test_sum(daft_df, pd_df):
    """Sums across an entire column for the entire table"""
    daft_df = daft_df.select(col("Unique Key").sum().alias("unique_key_sum"))
    pd_df = pd.DataFrame.from_records[{"unique_key_sum": [pd_df["Unique Key"].sum()]}]
    assert_df_equals(daft_df, pd_df)
