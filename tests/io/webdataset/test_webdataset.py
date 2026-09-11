from __future__ import annotations

import io
import tarfile
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

import daft
from daft import DataType, MediaType


class _QuietHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        pass


def _write_tar(path: Path, members: list[tuple[str, bytes]]) -> None:
    with tarfile.open(path, "w") as archive:
        for name, data in members:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))


@pytest.fixture
def webdataset_path(tmp_path: Path) -> Path:
    path = tmp_path / "samples.tar"
    _write_tar(
        path,
        [
            ("000001.jpg", b"first-image"),
            ("000001.json", b'{"caption": "first", "score": 1}'),
            ("000001.txt", b"first caption"),
            ("000001.cls", b"7"),
            ("000001.mp4", b"first-video"),
            ("000001.wav", b"first-audio"),
            ("000001.npy", b"first-array"),
            ("000002.jpg", b"second-image"),
            ("000002.json", b'{"caption": "second", "score": 2}'),
            ("000002.txt", b"second caption"),
            ("000002.cls", b"9"),
            ("000002.mp4", b"second-video"),
            ("000002.wav", b"second-audio"),
            ("000002.npy", b"second-array"),
        ],
    )
    return path


@pytest.fixture
def webdataset_http_url(webdataset_path: Path) -> str:
    handler = partial(_QuietHTTPRequestHandler, directory=str(webdataset_path.parent))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        yield f"http://127.0.0.1:{server.server_port}/{webdataset_path.name}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_read_webdataset(webdataset_path: Path) -> None:
    df = daft.read_webdataset(str(webdataset_path))

    assert df.schema()["jpg"].dtype == DataType.file(MediaType.image())
    assert df.schema()["mp4"].dtype == DataType.file(MediaType.video())
    assert df.schema()["wav"].dtype == DataType.file(MediaType.audio())
    assert df.schema()["npy"].dtype == DataType.file()
    assert df.schema()["txt"].dtype == DataType.string()
    assert df.schema()["cls"].dtype == DataType.int64()

    result = df.collect().to_pydict()
    assert result["__key__"] == ["000001", "000002"]
    assert result["__url__"] == [webdataset_path.as_uri()] * 2
    assert result["json"] == [
        {"caption": "first", "score": 1},
        {"caption": "second", "score": 2},
    ]
    assert result["txt"] == ["first caption", "second caption"]
    assert result["cls"] == [7, 9]

    images = result["jpg"]
    assert all(isinstance(image, daft.ImageFile) for image in images)
    with images[0].open() as image:
        assert image.read() == b"first-image"
    with images[1].open() as image:
        assert image.read() == b"second-image"

    assert all(isinstance(video, daft.VideoFile) for video in result["mp4"])
    assert all(isinstance(audio, daft.AudioFile) for audio in result["wav"])
    assert all(type(array) is daft.File for array in result["npy"])
    with result["mp4"][0].open() as video:
        assert video.read() == b"first-video"
    with result["wav"][0].open() as audio:
        assert audio.read() == b"first-audio"
    with result["npy"][0].open() as array:
        assert array.read() == b"first-array"


def test_read_webdataset_http_range_references(webdataset_http_url: str) -> None:
    result = daft.read_webdataset(webdataset_http_url).select("__key__", "jpg").collect().to_pydict()

    assert result["__key__"] == ["000001", "000002"]
    assert result["jpg"][0].path == webdataset_http_url
    assert result["jpg"][0].position is not None
    assert result["jpg"][0]._inner.size() == len(b"first-image")


def test_read_webdataset_projection(webdataset_path: Path) -> None:
    result = daft.read_webdataset(str(webdataset_path)).select("txt", "jpg").collect().to_pydict()

    assert set(result) == {"txt", "jpg"}
    assert result["txt"] == ["first caption", "second caption"]
    with result["jpg"][0].open() as image:
        assert image.read() == b"first-image"


def test_read_webdataset_directory_and_multiple_shards(tmp_path: Path) -> None:
    _write_tar(tmp_path / "first.tar", [("a.txt", b"first")])
    _write_tar(tmp_path / "second.tar", [("b.txt", b"second")])

    result = daft.read_webdataset(str(tmp_path), batch_size=1).sort("__key__").collect().to_pydict()

    assert result["__key__"] == ["a", "b"]
    assert result["txt"] == ["first", "second"]


def test_read_webdataset_preserves_multipart_suffixes(tmp_path: Path) -> None:
    path = tmp_path / "multipart.tar"
    _write_tar(
        path,
        [
            ("nested/sample.en.txt", b"caption"),
            ("nested/sample.jpg", b"image"),
        ],
    )

    result = daft.read_webdataset(str(path)).collect().to_pydict()

    assert result["__key__"] == ["nested/sample"]
    assert result["en.txt"] == ["caption"]
    with result["jpg"][0].open() as image:
        assert image.read() == b"image"


def test_read_webdataset_missing_inferred_field_is_null(tmp_path: Path) -> None:
    path = tmp_path / "missing.tar"
    _write_tar(
        path,
        [
            ("a.jpg", b"image-a"),
            ("a.txt", b"caption-a"),
            ("b.jpg", b"image-b"),
        ],
    )

    result = daft.read_webdataset(str(path)).sort("__key__").collect().to_pydict()

    assert result["txt"] == ["caption-a", None]


def test_read_webdataset_rejects_field_missing_from_inferred_schema(tmp_path: Path) -> None:
    path = tmp_path / "inconsistent-fields.tar"
    members = [(f"{index:06d}.txt", b"caption") for index in range(6)]
    members.append(("000005.json", b'{"caption": "late"}'))
    _write_tar(path, members)

    with pytest.raises(ValueError, match="was not present during schema inference"):
        daft.read_webdataset(str(path)).collect()


def test_read_webdataset_rejects_incompatible_json_schema(tmp_path: Path) -> None:
    path = tmp_path / "inconsistent-json.tar"
    members = [(f"{index:06d}.json", b'{"caption": "consistent"}') for index in range(5)]
    members.append(("000005.json", b'{"different": "field"}'))
    _write_tar(path, members)

    with pytest.raises(ValueError, match="does not match its inferred schema"):
        daft.read_webdataset(str(path)).collect()


def test_read_webdataset_rejects_duplicate_fields(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.tar"
    _write_tar(path, [("a.txt", b"first"), ("a.txt", b"second")])

    with pytest.raises(ValueError, match="Duplicate WebDataset field"):
        daft.read_webdataset(str(path))


def test_read_webdataset_rejects_compressed_tar(tmp_path: Path) -> None:
    path = tmp_path / "samples.tar.gz"
    with tarfile.open(path, "w:gz") as archive:
        info = tarfile.TarInfo("a.txt")
        info.size = 5
        archive.addfile(info, io.BytesIO(b"hello"))

    with pytest.raises(ValueError, match="Compressed WebDataset shards are not supported"):
        daft.read_webdataset(str(path))


@pytest.mark.parametrize("batch_size", [0, -1])
def test_read_webdataset_rejects_invalid_batch_size(webdataset_path: Path, batch_size: int) -> None:
    with pytest.raises(ValueError, match="batch_size must be greater than zero"):
        daft.read_webdataset(str(webdataset_path), batch_size=batch_size)
