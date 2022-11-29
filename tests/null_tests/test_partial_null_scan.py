from __future__ import annotations

from daft import DataFrame
from tests.assets.assets import SERVICE_REQUESTS_PARTIAL_EMPTY_CSV_FOLDER


def test_empty_partial_scan():
    df = DataFrame.read_csv(SERVICE_REQUESTS_PARTIAL_EMPTY_CSV_FOLDER)
    df = df.where(df["Agency"] == "NYPD")
    df.to_pandas()
