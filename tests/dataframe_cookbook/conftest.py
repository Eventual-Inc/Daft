from typing import List, Tuple

import pandas as pd
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col

IRIS_CSV = "tests/assets/iris.csv"
SERVICE_REQUESTS_CSV = "tests/assets/311-service-requests.100.csv"
SERVICE_REQUESTS_CSV_FOLDER = "tests/assets/311-service-requests.100"
SERVICE_REQUESTS_PARQUET = "tests/assets/311-service-requests.100.parquet"
SERVICE_REQUESTS_PARQUET_FOLDER = "tests/assets/311-service-requests.100.parquet_folder"
COLUMNS = ["Unique Key", "Complaint Type", "Borough", "Created Date", "Descriptor"]
CsvPathAndColumns = Tuple[str, List[str]]


def parametrize_sort_desc(arg_name: str):
    """Test case fixture to be used as a decorator that injects the sort ordering"""

    def _wrapper(test_case):
        parameters = [False, True]
        return pytest.mark.parametrize(arg_name, parameters, ids=[f"Descending:{v}" for v in parameters])(test_case)

    return _wrapper


def parametrize_service_requests_csv_daft_df(test_case):
    """Adds a `daft_df` parameter to test cases which is provided as a DataFrame of 100 rows
    from the 311-service-requests dataset, in various loaded partition configurations.
    """
    one_partition_csv = DataFrame.from_csv(SERVICE_REQUESTS_CSV).select(*[col(c) for c in COLUMNS])
    two_partitions_csv = DataFrame.from_csv(SERVICE_REQUESTS_CSV_FOLDER).select(*[col(c) for c in COLUMNS])
    one_partition_pq = DataFrame.from_parquet(SERVICE_REQUESTS_PARQUET).select(*[col(c) for c in COLUMNS])
    two_partitions_pq = DataFrame.from_parquet(SERVICE_REQUESTS_PARQUET_FOLDER).select(*[col(c) for c in COLUMNS])
    return pytest.mark.parametrize(
        ["daft_df"],
        [
            pytest.param(one_partition_csv, id="Source:CSV,NumFiles:1"),
            pytest.param(two_partitions_csv, id="Source:CSV,NumFiles:2"),
            pytest.param(one_partition_pq, id="Source:Parquet,NumFiles:1"),
            pytest.param(two_partitions_pq, id="Source:Parquet,NumFiles:2"),
        ],
    )(test_case)


@pytest.fixture(scope="function")
def service_requests_csv_pd_df():
    return pd.read_csv(SERVICE_REQUESTS_CSV, keep_default_na=False)[COLUMNS]


def parametrize_service_requests_csv_repartition(test_case):
    """Adds a `n_repartitions` parameter to test cases which provides the number of
    partitions that the test case should repartition its dataset into for testing
    """
    return pytest.mark.parametrize(
        ["repartition_nparts"],
        [pytest.param(n, id=f"NumRepartitionParts:{n}") for n in [1, 30, 50, 100, 101]],
    )(test_case)
