from __future__ import annotations

from daft import DataFrame
from tests.assets.assets import SERVICE_REQUESTS_CSV_FOLDER


def test_filter_on_partial_empty_columns():
    df = DataFrame.read_csv(SERVICE_REQUESTS_CSV_FOLDER)
    df = df.where(df["Agency"] == "NYPD")
    pd_df = df.to_pandas()
    assert (pd_df["Agency"] == "NYPD").all()
