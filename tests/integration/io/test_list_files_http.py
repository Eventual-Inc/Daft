from __future__ import annotations

from pathlib import Path

import pytest
from fsspec.implementations.http import HTTPFileSystem

from daft.daft import io_list
from tests.integration.io.conftest import mount_data_nginx


def compare_http_result(daft_ls_result: list, fsspec_result: list):
    daft_files = [(f["path"], f["type"].lower()) for f in daft_ls_result]
    httpfs_files = [(f["name"], f["type"]) for f in fsspec_result]
    assert len(daft_files) == len(httpfs_files)
    assert sorted(daft_files) == sorted(httpfs_files)


@pytest.fixture(scope="module")
def nginx_http_url(nginx_config, tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp("test-list-http")
    data_path = Path(tmpdir)
    (Path(data_path) / "file.txt").touch()
    (Path(data_path) / "test_ls").mkdir()
    (Path(data_path) / "test_ls" / "file.txt").touch()
    (Path(data_path) / "test_ls" / "paginated-10-files").mkdir()
    for i in range(10):
        (Path(data_path) / "test_ls" / "paginated-10-files" / f"file.{i}.txt").touch()

    with mount_data_nginx(nginx_config, data_path):
        yield nginx_config[0]


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path",
    [
        f"",
        f"/",
        f"test_ls",
        f"test_ls/",
        f"test_ls/paginated-10-files/",
    ],
)
def test_http_flat_directory_listing(path, nginx_http_url):
    http_path = f"{nginx_http_url}/{path}"
    fs = HTTPFileSystem()
    fsspec_result = fs.ls(http_path, detail=True)
    daft_ls_result = io_list(http_path)
    compare_http_result(daft_ls_result, fsspec_result)


@pytest.mark.integration()
def test_gs_single_file_listing(nginx_http_url):
    path = f"{nginx_http_url}/test_ls/file.txt"
    daft_ls_result = io_list(path)

    # NOTE: FSSpec will return size 0 list for this case, but we want to return 1 element to be
    # consistent with behavior of our other file listing utilities
    # fs = HTTPFileSystem()
    # fsspec_result = fs.ls(path, detail=True)

    assert len(daft_ls_result) == 1
    assert daft_ls_result[0] == {"path": path, "size": None, "type": "File"}


@pytest.mark.integration()
def test_http_notfound(nginx_http_url):
    path = f"{nginx_http_url}/test_ls/MISSING"
    fs = HTTPFileSystem()
    with pytest.raises(FileNotFoundError, match=path):
        fs.ls(path, detail=True)

    with pytest.raises(FileNotFoundError, match=path):
        io_list(path)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path",
    [
        f"",
        f"/",
    ],
)
def test_http_flat_directory_listing_recursive(path, nginx_http_url):
    http_path = f"{nginx_http_url}/{path}"
    fs = HTTPFileSystem()
    fsspec_result = list(fs.glob(http_path.rstrip("/") + "/**", detail=True).values())
    daft_ls_result = io_list(http_path, recursive=True)
    compare_http_result(daft_ls_result, fsspec_result)
