from __future__ import annotations

import os
from typing import Any

import pytest

import daft
from daft.functions import to_struct
from daft.io import FilenameProvider


def test_upload_local(tmpdir):
    bytes_data = [b"a", b"b", b"c"]
    data = {"data": bytes_data}
    df = daft.from_pydict(data)
    df = df.with_column("files", df["data"].upload(str(tmpdir + "/nested")))
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
    df = df.with_column("files", df["data"].upload(df["paths"]))
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
    df = df.with_column("files", df["data"].upload(df["paths"]))
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
    df_raise_error = df.with_column("files", df["data"].upload(df["paths"]))
    with pytest.raises(daft.exceptions.DaftCoreException):
        df_raise_error.collect()
    # Retry with `on_error` set to `null`.
    df_null = df.with_column("files", df["data"].upload(df["paths"], on_error="null"))
    df_null.collect()
    results = df_null.to_pydict()
    for path, expected_path in zip(results["files"], expected_paths):
        assert (path is None and expected_path is None) or path == expected_path
        if path is not None:
            assert path.startswith("file://")
            path = path[len("file://") :]
            with open(path, "rb") as f:
                assert f.read() == expected_data


def test_upload_local_filename_provider_row_from_columns(tmpdir, image_data_folder) -> None:
    folder = os.path.join(str(tmpdir), "uploads")

    class SourcePathFilenameProvider(FilenameProvider):
        def __init__(self, file_format: str):
            self.file_format = file_format

        def get_filename_for_block(
            self,
            write_uuid: str,
            task_index: int,
            block_index: int,
            file_idx: int,
            ext: str,
        ) -> str:
            raise NotImplementedError

        def get_filename_for_row(
            self,
            row: dict[str, Any],
            write_uuid: str,
            task_index: int,
            block_index: int,
            row_index: int,
            ext: str,
        ) -> str:
            source_path = row["source_path"]
            stem = os.path.splitext(os.path.basename(source_path))[0]
            tag = f"{stem}_converted"
            return f"{tag}_{write_uuid}_{task_index:06d}_{block_index:06d}_{row_index:06d}.{self.file_format}"

    provider = SourcePathFilenameProvider("jpeg")

    image_paths = sorted(str(p) for p in image_data_folder.iterdir())
    df = daft.from_pydict({"source_path": image_paths})
    df = df.with_column("data", df["source_path"].download())
    df = df.with_column(
        "dest",
        df["data"].upload(
            folder,
            filename_provider=provider,
            filename_provider_row=to_struct(source_path=df["source_path"]),
        ),
    )
    result = df.collect().to_pydict()
    paths = result["dest"]
    assert len(paths) == len(image_paths)
    for src, dest in zip(image_paths, paths):
        assert dest.startswith("file://")
        local_path = dest[len("file://") :]
        assert local_path.endswith(".jpeg")
        assert os.path.dirname(local_path) == folder
        stem = os.path.splitext(os.path.basename(src))[0]
        assert os.path.basename(local_path).startswith(f"{stem}_converted_")


def test_upload_local_image_jpeg_suffix(tmpdir, image_data: bytes) -> None:
    folder = os.path.join(str(tmpdir), "uploads")
    path = os.path.join(folder, "image-0.jpeg")

    df = daft.from_pydict({"data": [image_data], "path": [path]})
    df = df.with_column("dest", df["data"].upload(df["path"]))
    result = df.collect().to_pydict()

    assert result["dest"] == [f"file://{path}"]
    with open(path, "rb") as f:
        assert f.read() == image_data


def test_upload_local_filename_provider_row_specific_paths(tmpdir) -> None:
    class RecordingRowFilenameProvider(FilenameProvider):
        def __init__(self) -> None:
            self.row_calls: list[tuple[dict[str, Any], str, int, int, int, str]] = []

        def get_filename_for_block(
            self,
            write_uuid: str,
            task_index: int,
            block_index: int,
            file_idx: int,
            ext: str,
        ) -> str:
            raise AssertionError("get_filename_for_block should not be called for row uploads")

        def get_filename_for_row(
            self,
            row: dict[str, Any],
            write_uuid: str,
            task_index: int,
            block_index: int,
            row_index: int,
            ext: str,
        ) -> str:
            self.row_calls.append((row, write_uuid, task_index, block_index, row_index, ext))
            return f"upload-{row_index}"

    data_bytes = [b"one", b"two"]
    paths = [os.path.join(str(tmpdir), f"target-{i}.bin") for i in range(len(data_bytes))]

    df = daft.from_pydict({"data": data_bytes, "path": paths})

    provider = RecordingRowFilenameProvider()
    df = df.with_column("dest", df["data"].upload(df["path"], filename_provider=provider))
    result = df.collect().to_pydict()

    returned_paths = result["dest"]
    assert returned_paths == [f"file://{p}" for p in paths]

    assert provider.row_calls == []
