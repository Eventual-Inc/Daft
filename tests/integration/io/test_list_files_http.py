from __future__ import annotations

from pathlib import Path

import pytest
from fsspec.implementations.http import HTTPFileSystem

from daft.daft import io_glob
from tests.integration.io.conftest import mount_data_nginx


def compare_http_result(daft_ls_result: list, fsspec_result: list):
    daft_files = [(f["path"], f["type"].lower(), f["size"]) for f in daft_ls_result]
    httpfs_files = [(f["name"], f["type"], f["size"]) for f in fsspec_result]

    # io_glob doesn't return directory entries
    httpfs_files = [(p, t, s) for p, t, s in httpfs_files if t == "file"]

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
        f"/test_ls",
        f"/test_ls/",
        f"/test_ls//",
        f"/test_ls/paginated-10-files/",
    ],
)
def test_http_flat_directory_listing(path, nginx_http_url):
    http_path = f"{nginx_http_url}{path}"
    fs = HTTPFileSystem()
    fsspec_result = fs.ls(http_path, detail=True)
    daft_ls_result = io_glob(http_path)
    compare_http_result(daft_ls_result, fsspec_result)


@pytest.mark.integration()
def test_http_single_file_listing(nginx_http_url):
    path = f"{nginx_http_url}/test_ls/file.txt"
    daft_ls_result = io_glob(path)

    # NOTE: FSSpec will return size 0 list for this case, but we want to return 1 element to be
    # consistent with behavior of our other file listing utilities
    # fs = HTTPFileSystem()
    # fsspec_result = fs.ls(path, detail=True)

    assert len(daft_ls_result) == 1
    assert daft_ls_result[0] == {"path": path, "size": 0, "type": "File"}


@pytest.mark.integration()
def test_http_notfound(nginx_http_url):
    path = f"{nginx_http_url}/test_ls/MISSING"
    fs = HTTPFileSystem()
    with pytest.raises(FileNotFoundError, match=path):
        fs.ls(path, detail=True)

    with pytest.raises(FileNotFoundError, match=path):
        io_glob(path)


@pytest.mark.integration()
def test_http_flat_directory_listing_recursive(nginx_http_url):
    http_path = f"{nginx_http_url}/**"
    fs = HTTPFileSystem()
    fsspec_result = list(fs.glob(http_path, detail=True).values())
    daft_ls_result = io_glob(http_path)
    compare_http_result(daft_ls_result, fsspec_result)


@pytest.mark.integration()
def test_http_listing_absolute_urls(nginx_config, tmpdir):
    nginx_http_url, _ = nginx_config

    tmpdir = Path(tmpdir)
    test_manifest_file = tmpdir / "index.html"
    test_manifest_file.write_text(
        f"""
        <a href="{nginx_http_url}/other.html">this is an absolute path to a file</a>
        <a href="{nginx_http_url}/dir/">this is an absolute path to a dir</a>
    """
    )

    with mount_data_nginx(nginx_config, tmpdir):
        http_path = f"{nginx_http_url}/index.html"
        daft_ls_result = io_glob(http_path)

        # NOTE: Cannot use fsspec here because they do not correctly find the links
        # fsspec_result = fs.ls(http_path, detail=True)
        # compare_http_result(daft_ls_result, fsspec_result)

        assert daft_ls_result == [
            {"type": "File", "path": f"{nginx_http_url}/other.html", "size": None},
        ]


@pytest.mark.integration()
def test_http_listing_absolute_base_urls(nginx_config, tmpdir):
    nginx_http_url, _ = nginx_config

    tmpdir = Path(tmpdir)
    test_manifest_file = tmpdir / "index.html"
    test_manifest_file.write_text(
        f"""
        <a href="/other.html">this is an absolute base path to a file</a>
        <a href="/dir/">this is an absolute base path to a dir</a>
    """
    )

    with mount_data_nginx(nginx_config, tmpdir):
        http_path = f"{nginx_http_url}/index.html"
        daft_ls_result = io_glob(http_path)

        # NOTE: Cannot use fsspec here because they do not correctly find the links
        # fsspec_result = fs.ls(http_path, detail=True)
        # compare_http_result(daft_ls_result, fsspec_result)

        assert daft_ls_result == [
            {"type": "File", "path": f"{nginx_http_url}/other.html", "size": None},
        ]
