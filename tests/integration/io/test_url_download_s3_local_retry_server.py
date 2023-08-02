from __future__ import annotations

import pytest

import daft


@pytest.mark.integration()
@pytest.mark.skip(
    reason="""[IO-RETRIES] This currently fails: we need better retry policies to have this work consistently.
                  Currently, if all the retries for a given URL happens to land in the same 1-second window, the request fails.
                  We should be able to get around this with a more generous retry policy, with larger increments between backoffs.
                  """
)
def test_url_download_local_retry_server(retry_server_s3_config):
    bucket = "80-per-second-rate-limited-gets-bucket"
    data = {"urls": [f"s3://{bucket}/foo{i}" for i in range(100)]}
    df = daft.from_pydict(data)
    df = df.with_column(
        "data", df["urls"].url.download(io_config=retry_server_s3_config, use_native_downloader=True, on_error="null")
    )
    assert df.to_pydict() == {**data, "data": [f"foo{i}".encode() for i in range(100)]}
