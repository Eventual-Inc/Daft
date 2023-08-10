from __future__ import annotations

import pytest

import daft


@pytest.mark.integration()
def test_url_download_public_azure(azure_storage_public_config) -> None:
    data = {"urls": ["az://public-anonymous/mvp.parquet"]}
    df = daft.from_pydict(data)
    df = df.with_column(
        "data", df["urls"].url.download(io_config=azure_storage_public_config, use_native_downloader=True)
    )

    data = df.to_pydict()
    assert len(data["data"]) == 1
    for databytes in data["data"]:
        assert databytes is not None
