from __future__ import annotations

import pandas as pd
import pytest

import daft
from tests.integration.sql.conftest import TRINO_URL


@pytest.mark.integration()
def test_trino_create_dataframe_ok(check_db_server_initialized) -> None:
    if check_db_server_initialized:
        df = daft.read_sql("SELECT * FROM tpch.sf1.nation", TRINO_URL)
        pd_df = pd.read_sql("SELECT * FROM tpch.sf1.nation", TRINO_URL)
        assert df.to_pandas().equals(pd_df)
