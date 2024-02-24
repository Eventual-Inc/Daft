from __future__ import annotations

import pandas as pd
import pytest

import daft


@pytest.mark.integration()
def test_trino_create_dataframe_ok(db_url) -> None:
    url = db_url("trino")
    df = daft.read_sql("SELECT * FROM tpch.sf1.nation", url)
    pd_df = pd.read_sql("SELECT * FROM tpch.sf1.nation", url)

    assert df.equals(pd_df)
