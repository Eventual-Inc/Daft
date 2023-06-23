from __future__ import annotations

import socketserver
import time
from http.server import BaseHTTPRequestHandler
from multiprocessing import Process
from socket import socket

import pytest
import requests
from aiohttp.client_exceptions import ClientResponseError

import daft
from tests.integration.io.conftest import YieldFixture


def _get_free_port():
    """Helper to get a free port number - may be susceptible to race conditions,
    but is likely good enough for our unit testing usecase.
    """
    with socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _wait_for_server(ready_url: str, max_wait_time_s: int = 1):
    """Waits for a server to be up and serving 200's from the provided URL"""
    SLEEP_INTERVAL = 0.1
    for _ in range(int(max_wait_time_s / SLEEP_INTERVAL)):
        try:
            if requests.get(ready_url).status_code == 200:
                break
        except requests.exceptions.ConnectionError:
            time.sleep(SLEEP_INTERVAL)
    else:
        raise RuntimeError("Timed out while waiting for mock HTTP server fixture to be ready")


def _serve_error_server(code, port):
    """Target function for serving a HTTP service that always throws the specified error code"""

    class MyAlwaysThrowHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/ready":
                self.send_response(200)
                self.end_headers()
                return
            self.send_response(code)
            self.end_headers()
            self.wfile.write(b"Some message")

    with socketserver.TCPServer(("", port), MyAlwaysThrowHandler) as httpd:
        httpd.serve_forever()


@pytest.fixture(
    scope="function",
    params=[
        # Unauthorized
        401,
        # Forbidden
        403,
        # Not found
        404,
        # Too many requests
        429,
        # Internal server error
        500,
        # Service unavailable
        503,
    ],
)
def mock_error_http_server(request) -> YieldFixture[tuple[str, int]]:
    """Provides a mock HTTP server that throws various HTTP status code errors when receiving any GET requests

    This fixture yields a tuple of:
        str: URL to the HTTP server
        int: HTTP status code that it throws when accessed with a GET request at any path
    """
    code = request.param
    port = _get_free_port()
    url = f"http://localhost:{port}"

    p = Process(target=_serve_error_server, args=(code, port))
    p.start()
    try:
        _wait_for_server(f"{url}/ready")
        yield (url, code)
    finally:
        p.terminate()
        p.join()


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_url_download_http(mock_http_image_urls, image_data, use_native_downloader):
    data = {"urls": mock_http_image_urls}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(use_native_downloader=use_native_downloader))
    assert df.to_pydict() == {**data, "data": [image_data for _ in range(len(mock_http_image_urls))]}


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_url_download_http_error_codes(mock_error_http_server, use_native_downloader):
    url, code = mock_error_http_server
    data = {"urls": [f"{url}/missing.jpeg"]}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(on_error="raise", use_native_downloader=use_native_downloader))

    # 404 should always be corner-cased to return FileNotFoundError regardless of I/O implementation
    if code == 404:
        with pytest.raises(FileNotFoundError):
            df.collect()
    # When using fsspec, other error codes are bubbled up to the user as aiohttp.client_exceptions.ClientResponseError
    elif not use_native_downloader:
        with pytest.raises(ClientResponseError) as e:
            df.collect()
        assert e.value.code == code
    # When using native downloader, we throw a ValueError
    else:
        with pytest.raises(ValueError) as e:
            df.collect()
        # NOTE: We may want to add better errors in the future to provide a better
        # user-facing I/O error with the error code
        assert f"Status({code})" in str(e.value)
