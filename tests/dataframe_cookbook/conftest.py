from typing import Any, Dict, List, Tuple, Union

import pandas as pd
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from tests.conftest import run_tdd

IRIS_CSV = "tests/assets/iris.csv"
SERVICE_REQUESTS_CSV = "tests/assets/311-service-requests.50.csv"
COLUMNS = ["Unique Key", "Complaint Type", "Borough", "Created Date", "Descriptor"]
CsvPathAndColumns = Tuple[str, List[str]]


def parametrize_sort_desc(arg_name: str):
    """Test case fixture to be used as a decorator that injects the sort ordering"""

    def _wrapper(test_case):
        parameters = [False]
        if run_tdd():
            parameters.extend([True])
        return pytest.mark.parametrize(arg_name, parameters)(test_case)

    return _wrapper


def parametrize_partitioned_daft_df(
    source: Union[CsvPathAndColumns, Dict[str, List[Any]]] = (SERVICE_REQUESTS_CSV, COLUMNS)
):
    """Test case fixture to be used as a decorator that constructs and parametrizes a test with the appropriate DaFt/pandas DataFrames

    Usage:

    >>> # To use default CSV at tests/assets/311-service-requests.1000.csv as the datasource
    >>> @parametrize_partitioned_daft_df
    >>> def test_foo(daft_df, pd_df):
    >>>     ...

    >>> # To use a dictionary as the datasource
    >>> @parametrize_partitioned_daft_df(source={"foo": [i for i in range(1000)]})
    >>> def test_foo(daft_df, pd_df):
    >>>     ...
    """

    if isinstance(source, tuple):
        csv_path, columns = source
        base_df = DataFrame.from_csv(csv_path).select(*[col(c) for c in columns])
        pd_df = pd.read_csv(csv_path, keep_default_na=False)[columns]
    elif isinstance(source, dict):
        wrong_lengths = {key: len(source[key]) != 1000 for key in source}
        if any(list(wrong_lengths.values())):
            raise RuntimeError("@parametrize_partitioned_daft_df must be used with dataframes of 1,000 rows")
        base_df = DataFrame.from_pydict(source)
        pd_df = pd.DataFrame.from_dict(source)
    else:
        raise NotImplementedError(f"Datasource not supported: {source}")

    def _wrapper(test_case):
        daft_dfs = [
            base_df,
            base_df.repartition(1),  # Single partition
            base_df.repartition(10),  # 5 partitions of 10 each
            base_df.repartition(20),  # Uneven partitions
            base_df.repartition(50),  # One row per parittion
            base_df.repartition(51),  # One empty partition
        ]
        return pytest.mark.parametrize(["daft_df", "pd_df"], [(daft_df, pd_df.copy()) for daft_df in daft_dfs])(
            test_case
        )

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
