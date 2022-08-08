from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col

IRIS_CSV = "tests/assets/iris.csv"
SERVICE_REQUESTS_CSV = "tests/assets/311-service-requests.50.csv"
COLUMNS = ["Unique Key", "Complaint Type", "Borough", "Created Date", "Descriptor"]
CsvPathAndColumns = Tuple[str, List[str]]


def parametrize_sort_desc(arg_name: str):
    """Test case fixture to be used as a decorator that injects the sort ordering"""

    def _wrapper(test_case):
        parameters = [False, True]
        return pytest.mark.parametrize(arg_name, parameters, ids=[f"Descending:{v}" for v in parameters])(test_case)

    return _wrapper


def parametrize_partitioned_daft_df(
    source: Union[CsvPathAndColumns, Dict[str, List[Any]]] = (SERVICE_REQUESTS_CSV, COLUMNS),
    partitioning: Optional[List[int]] = [],
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
        if pd_df.shape[0] != 49:
            raise NotImplementedError("Only supports CSVs of 50 rows")
        if not partitioning:
            partitioning = [
                1,  # Single partition
                10,  # 5 partitions of 10 each
                20,  # Uneven partitions
                50,  # One row per parittion
                51,  # One empty partition
            ]
    elif isinstance(source, dict):
        base_df = DataFrame.from_pydict(source)
        pd_df = pd.DataFrame.from_dict(source)
    else:
        raise NotImplementedError(f"Datasource not supported: {source}")

    def _wrapper(test_case):
        daft_dfs = [base_df] + [base_df.repartition(i) for i in partitioning]
        ids = [f"Repartition:{num}" for num in ["None", *partitioning]]
        return pytest.mark.parametrize(
            ["daft_df", "pd_df"],
            [pytest.param(daft_df, pd_df.copy(), id=test_id) for test_id, daft_df in zip(ids, daft_dfs)],
        )(test_case)

    return _wrapper
