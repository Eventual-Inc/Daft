from __future__ import annotations

import os

import pytest

import daft


def test_upload_local(tmpdir):
    bytes_data = [b"a", b"b", b"c"]
    data = {"data": bytes_data}
    df = daft.from_pydict(data)
    df = df.with_column("files", df["data"].url.upload(str(tmpdir + "/nested")))
    df.collect()

    results = df.to_pydict()
    assert results["data"] == bytes_data
    assert len(results["files"]) == len(bytes_data)
    for path, expected in zip(results["files"], bytes_data):
        assert path.startswith("file://")
        path = path[len("file://") :]
        with open(path, "rb") as f:
            assert f.read() == expected


def test_upload_local_single_file_url(tmpdir):
    bytes_data = [b"a"]
    paths = [f"{tmpdir}/0"]
    data = {"data": bytes_data, "paths": paths}
    df = daft.from_pydict(data)
    # Even though there is only one row, since we pass in the upload URL via an expression, we
    # should treat the given path as a per-row path and write directly to that path, instead of
    # treating the path as a directory and writing to `{path}/uuid`.
    df = df.with_column("files", df["data"].url.upload(df["paths"]))
    df.collect()

    results = df.to_pydict()
    assert results["data"] == bytes_data
    assert len(results["files"]) == len(bytes_data)
    for path, expected in zip(results["files"], bytes_data):
        assert path.startswith("file://")
        path = path[len("file://") :]
        with open(path, "rb") as f:
            assert f.read() == expected
    # Check that data was uploaded to the correct paths.
    for path, expected in zip(results["files"], paths):
        assert path == "file://" + expected


def test_upload_local_row_specifc_urls(tmpdir):
    bytes_data = [b"a", b"b", b"c"]
    paths = [f"{tmpdir}/0", f"{tmpdir}/1", f"{tmpdir}/2"]
    data = {"data": bytes_data, "paths": paths}
    df = daft.from_pydict(data)
    df = df.with_column("files", df["data"].url.upload(df["paths"]))
    df.collect()

    results = df.to_pydict()
    assert results["data"] == bytes_data
    assert len(results["files"]) == len(bytes_data)
    for path, expected in zip(results["files"], bytes_data):
        assert path.startswith("file://")
        path = path[len("file://") :]
        with open(path, "rb") as f:
            assert f.read() == expected
    # Check that data was uploaded to the correct paths.
    for path, expected in zip(results["files"], paths):
        assert path == "file://" + expected


@pytest.mark.skipif(os.geteuid() == 0, reason="Skipping test when run as root user")
def test_upload_local_no_write_permissions(tmpdir):
    bytes_data = [b"a", b"b", b"c"]
    # We have no write permissions to the first and third paths.
    paths = ["/some-root-path", f"{tmpdir}/normal_path", "/another-bad-path"]
    expected_paths = [None, f"file://{tmpdir}/normal_path", None]
    expected_data = b"b"
    data = {"data": bytes_data, "paths": paths}
    df = daft.from_pydict(data)
    df_raise_error = df.with_column("files", df["data"].url.upload(df["paths"]))
    with pytest.raises(daft.exceptions.DaftCoreException):
        df_raise_error.collect()
    # Retry with `on_error` set to `null`.
    df_null = df.with_column("files", df["data"].url.upload(df["paths"], on_error="null"))
    df_null.collect()
    results = df_null.to_pydict()
    for path, expected_path in zip(results["files"], expected_paths):
        assert (path is None and expected_path is None) or path == expected_path
        if path is not None:
            assert path.startswith("file://")
            path = path[len("file://") :]
            with open(path, "rb") as f:
                assert f.read() == expected_data
