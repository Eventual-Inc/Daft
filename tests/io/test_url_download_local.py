from __future__ import annotations

import pathlib

import pytest

import daft
from tests.integration.io.conftest import YieldFixture


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


@pytest.mark.integration()
def test_url_download_local(local_image_data_fixture, image_data):
    data = {"urls": local_image_data_fixture}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download())
    assert df.to_pydict() == {**data, "data": [image_data for _ in range(len(local_image_data_fixture))]}


@pytest.mark.integration()
def test_url_download_local_missing(local_image_data_fixture):
    data = {"urls": local_image_data_fixture + ["/missing/path/x.jpeg"]}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(on_error="raise"))

    with pytest.raises(FileNotFoundError):
        df.collect()


@pytest.mark.integration()
def test_url_download_local_no_read_permissions(local_image_data_fixture, tmpdir):
    bad_permission_filepath = pathlib.Path(tmpdir) / "bad_file.jpeg"
    bad_permission_filepath.write_bytes(b"foo")
    bad_permission_filepath.chmod(0)

    data = {"urls": local_image_data_fixture + [str(bad_permission_filepath)]}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(on_error="raise"))

    with pytest.raises(ValueError, match="Permission denied"):
        df.collect()
