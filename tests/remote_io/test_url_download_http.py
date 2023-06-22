from __future__ import annotations

import pathlib
import subprocess
import time

import pytest

import daft
from tests.remote_io.conftest import YieldFixture


@pytest.fixture(scope="session")
def mock_http_server(tmp_path_factory) -> YieldFixture[tuple[str, pathlib.Path]]:
    """Provides a mock HTTP server that serves files in a given directory

    This fixture yields a tuple of:
        str: URL to the HTTP server
        pathlib.Path: tmpdir to place files into, which will then be served by the HTTP server
    """

    tmpdir = tmp_path_factory.mktemp("data")

    PORT = 8000
    proc = subprocess.Popen(["python", "-m", "http.server", "-d", str(tmpdir), str(PORT)])

    # Give the server some time to spin up
    time.sleep(0.2)

    yield (f"http://localhost:{PORT}", tmpdir)

    proc.kill()


@pytest.fixture(scope="function")
def http_image_data_fixture(mock_http_server, image_data) -> YieldFixture[list[str]]:
    """Populates the mock HTTP server with some fake data and returns filepaths"""
    # Dump some images into the tmpdir
    server_url, tmpdir = mock_http_server
    urls = []
    for i in range(10):
        path = tmpdir / f"{i}.jpeg"
        path.write_bytes(image_data)
        urls.append(f"{server_url}/{path.relative_to(tmpdir)}")

    yield urls

    # Cleanup tmpdir
    for child in tmpdir.glob("*"):
        child.unlink()


def test_url_download_http(http_image_data_fixture, image_data):
    data = {"urls": http_image_data_fixture}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download())
    assert df.to_pydict() == {**data, "data": [image_data for _ in range(len(http_image_data_fixture))]}
