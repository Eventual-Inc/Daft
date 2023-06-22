from __future__ import annotations

import pathlib

import pytest

import daft
from tests.remote_io.conftest import YieldFixture


@pytest.fixture(scope="function")
def local_image_data_fixture(tmpdir, image_data) -> YieldFixture[list[str]]:
    """Populates the local tmpdir with some fake data and returns filepaths"""
    # Dump some images into the tmpdir
    tmpdir = pathlib.Path(tmpdir)
    urls = []
    for i in range(10):
        path = tmpdir / f"{i}.jpeg"
        path.write_bytes(image_data)
        urls.append(str(path))

    yield urls

    # Cleanup tmpdir
    for child in tmpdir.glob("*"):
        child.unlink()


def test_url_download_local(local_image_data_fixture, image_data):
    data = {"urls": local_image_data_fixture}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download())
    assert df.to_pydict() == {**data, "data": [image_data for _ in range(len(local_image_data_fixture))]}
