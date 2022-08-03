import pandas as pd
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col

IRIS_CSV = "tests/assets/iris.csv"
SERVICE_REQUESTS_CSV = "tests/assets/311-service-requests.1000.csv"
COLUMNS = ["Unique Key", "Complaint Type", "Borough", "Created Date", "Descriptor"]


@pytest.fixture(scope="function")
def pd_df() -> pd.DataFrame:
    return pd.read_csv(SERVICE_REQUESTS_CSV, keep_default_na=False)[COLUMNS]


@pytest.fixture(scope="function")
def daft_df() -> DataFrame:
    return DataFrame.from_csv(SERVICE_REQUESTS_CSV).select(*[col(c) for c in COLUMNS])


def assert_df_equals(daft_df: DataFrame, pd_df: pd.DataFrame):
    """Asserts that a Daft Dataframe is equal to a Pandas Dataframe"""
    daft_df = daft_df.to_pandas().reset_index(drop=True).reindex(sorted(daft_df.column_names()), axis=1)
    pd_df = pd_df.reset_index(drop=True).reindex(sorted(pd_df.columns), axis=1)
    assert sorted(daft_df.columns) == sorted(pd_df.columns), f"Found {daft_df.columns} expected {pd_df.columns}"
    for col in pd_df.columns:
        df_series = daft_df[col]
        pd_series = pd_df[col]
        pd.testing.assert_series_equal(df_series, pd_series)
