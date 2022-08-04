import pandas as pd
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from tests.conftest import RUN_TDD_OPTION

IRIS_CSV = "tests/assets/iris.csv"
SERVICE_REQUESTS_CSV = "tests/assets/311-service-requests.1000.csv"
COLUMNS = ["Unique Key", "Complaint Type", "Borough", "Created Date", "Descriptor"]


@pytest.fixture(scope="function")
def pd_df() -> pd.DataFrame:
    return pd.read_csv(SERVICE_REQUESTS_CSV, keep_default_na=False)[COLUMNS]


def parametrize_sort_desc(arg_name: str):
    """Test case fixture to be used as a decorator that injects the sort ordering"""

    def _wrapper(test_case):
        parameters = [False]
        if RUN_TDD_OPTION:
            parameters.extend([True])
        return pytest.mark.parametrize(arg_name, parameters)(test_case)

    return _wrapper


def parametrize_partitioned_daft_df(arg_name: str):
    """Test case fixture to be used as a decorator that injects a DaFt dataframe with multiple different
    partitioning schemes

    Usage:

    @parametrize_partitioned_daft_df
    def test_foo(daft_df):

    """

    def _wrapper(test_case):
        base_df = DataFrame.from_csv(SERVICE_REQUESTS_CSV).select(*[col(c) for c in COLUMNS])
        parameters = [
            base_df,
            base_df.repartition(1),
        ]
        # TODO(jay): Change this once partition behavior is fixed
        if RUN_TDD_OPTION:
            parameters.extend(
                [
                    base_df.repartition(2),
                    base_df.repartition(10),
                    base_df.repartition(1000),
                    base_df.repartition(1001),
                ]
            )
        return pytest.mark.parametrize(arg_name, parameters)(test_case)

    return _wrapper


def assert_df_equals(
    daft_df: DataFrame, pd_df: pd.DataFrame, sort_key: str = "Unique Key", assert_ordering: bool = False
):
    """Asserts that a Daft Dataframe is equal to a Pandas Dataframe.

    By default, we do not assert that the ordering is equal and will sort dataframes according to `sort_key`.
    However, if asserting on ordering is intended behavior, set `assert_ordering=True` and this function will
    no longer run sorting before running the equality comparison.
    """
    daft_pd_df = daft_df.to_pandas().reset_index(drop=True).reindex(sorted(daft_df.column_names()), axis=1)
    pd_df = pd_df.reset_index(drop=True).reindex(sorted(pd_df.columns), axis=1)

    # If we are not asserting on the ordering being equal, we run a sort operation on both dataframes using the provided sort key
    if not assert_ordering:
        assert sort_key in daft_pd_df.columns, (
            f"DaFt Dataframe missing key: {sort_key}\nNOTE: This doesn't necessarily mean your code is "
            "breaking, but our testing utilities require sorting on this key in order to compare your "
            "Dataframe against the expected Pandas Dataframe."
        )
        assert sort_key in pd_df.columns, (
            f"Pandas Dataframe missing key: {sort_key}\nNOTE: This doesn't necessarily mean your code is "
            "breaking, but our testing utilities require sorting on this key in order to compare your "
            "Dataframe against the expected Pandas Dataframe."
        )
        daft_pd_df = daft_pd_df.sort_values(by=sort_key).reset_index(drop=True)
        pd_df = pd_df.sort_values(by=sort_key).reset_index(drop=True)

    assert sorted(daft_pd_df.columns) == sorted(pd_df.columns), f"Found {daft_pd_df.columns} expected {pd_df.columns}"
    for col in pd_df.columns:
        df_series = daft_pd_df[col]
        pd_series = pd_df[col]
        pd.testing.assert_series_equal(df_series, pd_series)
