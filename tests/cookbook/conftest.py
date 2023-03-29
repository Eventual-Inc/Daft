from __future__ import annotations

from typing import List, Tuple

import pandas as pd
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from tests_legacy.assets.assets import SERVICE_REQUESTS_CSV

COLUMNS = ["Unique Key", "Complaint Type", "Borough", "Created Date", "Descriptor"]
CsvPathAndColumns = Tuple[str, List[str]]


@pytest.fixture(scope="function")
def daft_df():
    return DataFrame.read_csv(SERVICE_REQUESTS_CSV).select(*[col(c) for c in COLUMNS])


@pytest.fixture(scope="function")
def service_requests_csv_pd_df():
    return pd.read_csv(SERVICE_REQUESTS_CSV, keep_default_na=False)[COLUMNS]


def parametrize_service_requests_csv_repartition(test_case):
    """Adds a `n_repartitions` parameter to test cases which provides the number of
    partitions that the test case should repartition its dataset into for testing
    """
    return pytest.mark.parametrize(
        ["repartition_nparts"],
        [pytest.param(n, id=f"NumRepartitionParts:{n}") for n in [1, 2]],
    )(test_case)
