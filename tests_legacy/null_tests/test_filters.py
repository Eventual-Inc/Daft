from __future__ import annotations

import pytest

from daft import DataFrame
from tests.assets.assets import (
    SERVICE_REQUESTS_CSV_FOLDER,
    SERVICE_REQUESTS_PARTIAL_EMPTY_CSV_FOLDER,
)


def test_filter_on_partial_empty_columns():
    df = DataFrame.read_csv(SERVICE_REQUESTS_CSV_FOLDER)
    df = df.where(df["Agency"] == "NYPD")
    pd_df = df.to_pandas()
    assert (pd_df["Agency"] == "NYPD").all()


@pytest.mark.skip
def test_filter_on_partial_empty_files():
    df = DataFrame.read_csv(SERVICE_REQUESTS_PARTIAL_EMPTY_CSV_FOLDER)
    df = df.where(df["Agency"] == "NYPD")
    pd_df = df.to_pandas()
    assert (pd_df["Agency"] == "NYPD").all()
