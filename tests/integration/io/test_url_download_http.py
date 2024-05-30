from __future__ import annotations

import pytest
from aiohttp.client_exceptions import ClientResponseError

import daft


@pytest.mark.integration()
@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_url_download_http(mock_http_image_urls, image_data, use_native_downloader):
    data = {"urls": mock_http_image_urls}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(use_native_downloader=use_native_downloader))
    assert df.to_pydict() == {**data, "data": [image_data for _ in range(len(mock_http_image_urls))]}


@pytest.mark.integration()
@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 429, 500, 503])
@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_url_download_http_error_codes(nginx_config, use_native_downloader, status_code):
    server_url, _ = nginx_config
    data = {"urls": [f"{server_url}/{status_code}.html"]}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(on_error="raise", use_native_downloader=use_native_downloader))

    skip_fsspec_downloader = daft.context.get_context().runner_config.name == "ray"

    # 404 should always be corner-cased to return FileNotFoundError regardless of I/O implementation
    if status_code == 404:
        with pytest.raises(FileNotFoundError) as exc_info:
            df.collect()

    # When using fsspec, other error codes are bubbled up to the user as aiohttp.client_exceptions.ClientResponseError
    elif not use_native_downloader:
        # Ray runner has a pretty catastrophic failure when raising non-pickleable exceptions (ClientResponseError is not pickleable)
        # See Ray issue: https://github.com/ray-project/ray/issues/36893
        if skip_fsspec_downloader:
            pytest.skip()

        with pytest.raises(RuntimeError) as exc_info:
            df.collect()

        # Ray's wrapping of the exception loses information about the `.cause`, but preserves it in the string error message
        if daft.context.get_context().runner_config.name == "ray":
            assert "ClientResponseError" in str(exc_info.value)
        else:
            assert isinstance(exc_info.value.__cause__, ClientResponseError)
            assert exc_info.value.__cause__.code == status_code
    # When using native downloader, we throw a ValueError
    else:
        # NOTE: We may want to add better errors in the future to provide a better
        # user-facing I/O error with the error code
        with pytest.raises(ValueError, match=f"{status_code}"):
            df.collect()
