from __future__ import annotations

from typing import List, Tuple

import pandas as pd
import pytest

import daft
from daft.expressions import col
from tests.cookbook.assets import COOKBOOK_DATA_CSV

COLUMNS = ["Unique Key", "Complaint Type", "Borough", "Created Date", "Descriptor"]
CsvPathAndColumns = Tuple[str, List[str]]


@pytest.fixture(scope="function")
def daft_df():
    return daft.read_csv(COOKBOOK_DATA_CSV).select(*[col(c) for c in COLUMNS])


@pytest.fixture(scope="function")
def service_requests_csv_pd_df():
    return pd.read_csv(COOKBOOK_DATA_CSV, keep_default_na=False)[COLUMNS]


@pytest.fixture(scope="module", params=[1, 2])
def repartition_nparts(request):
    """Adds a `n_repartitions` parameter to test cases which provides the number of
    partitions that the test case should repartition its dataset into for testing
    """
    return request.param
