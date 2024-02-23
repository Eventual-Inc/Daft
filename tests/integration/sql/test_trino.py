from __future__ import annotations

import pandas as pd
import pytest

import daft

URL = "trino://user@localhost:8080/tpch"


@pytest.mark.integration()
def test_trino_create_dataframe_ok() -> None:
    df = daft.read_sql("SELECT * FROM tpch.sf1.nation", URL)
    pd_df = pd.read_sql("SELECT * FROM tpch.sf1.nation", URL)
    assert df.to_pandas().equals(pd_df)
