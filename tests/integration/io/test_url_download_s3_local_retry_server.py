from __future__ import annotations

import pytest

import daft
from tests.utils import sort_pydict


@pytest.mark.integration()
def test_url_download_local_retry_server(retry_server_s3_config):
    bucket = "80-per-second-rate-limited-gets-bucket"
    data = {"urls": sorted([f"s3://{bucket}/foo{i}" for i in range(100)])}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(io_config=retry_server_s3_config, on_error="null"))
    assert sort_pydict(df.to_pydict(), "urls", ascending=True) == {
        **data,
        "data": [f"foo{i}".encode() for i in range(100)],
    }
