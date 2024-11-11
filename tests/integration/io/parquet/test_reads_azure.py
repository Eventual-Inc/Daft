from __future__ import annotations

import pytest

import daft


@pytest.mark.integration()
def test_read_azure_infer_storage_account():
    io_config1 = daft.io.IOConfig(azure=daft.io.AzureConfig(storage_account="dafttestdata", anonymous=True))
    io_config2 = daft.io.IOConfig(azure=daft.io.AzureConfig(anonymous=True))

    url = "abfss://public-anonymous@dafttestdata.dfs.core.windows.net/mvp.parquet"

    df1 = daft.read_parquet(url, io_config=io_config1)
    df2 = daft.read_parquet(url, io_config=io_config2)

    assert df1.schema() == df2.schema()
    assert df1.to_arrow() == df2.to_arrow()
