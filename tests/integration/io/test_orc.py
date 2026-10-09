from __future__ import annotations

import asyncio
import io
import threading
from collections.abc import Iterator
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pyarrow as pa
import pytest
from pyarrow import orc

import daft
from daft.io import _orc
from daft.io._orc import OrcSource
from daft.io.pushdowns import Pushdowns
from tests.integration.io.conftest import minio_create_bucket


def _orc_bytes(table: pa.Table) -> bytes:
    buffer = io.BytesIO()
    orc.write_table(table, buffer, stripe_size=65536, batch_size=1024)
    return buffer.getvalue()


class _QuietHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def orc_http_url(tmp_path: Path) -> Iterator[str]:
    (tmp_path / "data.orc").write_bytes(_orc_bytes(pa.table({"id": range(9), "name": [f"row-{i}" for i in range(9)]})))
    handler = partial(_QuietHTTPRequestHandler, directory=str(tmp_path))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/data.orc"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def orc_hive_http_urls(tmp_path: Path) -> Iterator[list[str]]:
    for p in [1, 2]:
        directory = tmp_path / f"p={p}"
        directory.mkdir()
        (directory / "data.orc").write_bytes(_orc_bytes(pa.table({"id": [p * 10, p * 10 + 1]})))
    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(_QuietHTTPRequestHandler, directory=str(tmp_path)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield [f"http://127.0.0.1:{server.server_port}/p={p}/data.orc" for p in [1, 2]]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _assert_hive_file_pruning(source: OrcSource, selected: str, monkeypatch) -> None:
    original = _orc._iter_orc_batches
    scanned: list[str] = []

    def tracking_batches(path, *args, **kwargs):
        scanned.append(path)
        yield from original(path, *args, **kwargs)

    async def read_selected() -> list[dict]:
        tasks = [task async for task in source.get_tasks(Pushdowns(partition_filters=daft.col("p") == 2))]
        assert [task._path for task in tasks] == [selected]
        return [batch.to_pydict() async for batch in tasks[0].read()]

    with monkeypatch.context() as patch:
        patch.setattr(_orc, "_iter_orc_batches", tracking_batches)
        assert asyncio.run(read_selected()) == [{"id": [20, 21], "p": [2, 2]}]
    assert scanned == [selected]


@pytest.mark.integration()
def test_read_orc_hive_http(orc_hive_http_urls: list[str], monkeypatch) -> None:
    source = OrcSource(orc_hive_http_urls, None, 2, hive_partitioning=True)
    _assert_hive_file_pruning(source, orc_hive_http_urls[1], monkeypatch)
    result = source.read().where((daft.col("p") == 2) & (daft.col("id") > 20)).select("id").to_pydict()
    assert result == {"id": [21]}


@pytest.mark.integration()
@pytest.mark.parametrize("kind", ["directory", "glob", "list"])
def test_read_orc_hive_s3(minio_io_config: daft.io.IOConfig, kind: str, monkeypatch) -> None:
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket):
        root = f"s3://{bucket}/orc-hive"
        paths = [f"{root}/p={p}/data.orc" for p in [1, 2]]
        for p, path in zip([1, 2], paths):
            fs.write_bytes(path, _orc_bytes(pa.table({"id": [p * 10, p * 10 + 1]})))
        source_path = {"directory": root, "glob": f"{root}/**/*.orc", "list": paths}[kind]
        source = OrcSource(source_path, minio_io_config, 2, hive_partitioning=True)
        _assert_hive_file_pruning(source, paths[1], monkeypatch)
        assert source.read().where(daft.col("p") == 2).select("id").sort("id").to_pydict() == {"id": [20, 21]}


@pytest.mark.integration()
def test_read_orc_hive_s3_null_and_conflict(minio_io_config: daft.io.IOConfig) -> None:
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket):
        paths = [f"s3://{bucket}/p=3/data.orc", f"s3://{bucket}/plain/data.orc"]
        fs.write_bytes(paths[0], _orc_bytes(pa.table({"id": [1], "p": [99]})))
        fs.write_bytes(paths[1], _orc_bytes(pa.table({"id": [2], "p": [88]})))
        df = daft.read_orc(paths, io_config=minio_io_config, hive_partitioning=True)
        assert df.sort("id").to_pydict() == {"id": [1, 2], "p": [3, None]}
        assert df.where(daft.col("p").is_null()).select("id").to_pydict() == {"id": [2]}


@pytest.mark.integration()
def test_read_orc_http(orc_http_url: str) -> None:
    df = daft.read_orc(orc_http_url, batch_size=2)
    assert df.count_rows() == 9
    assert df.where(daft.col("id") >= 6).select("name").limit(2).sort("name").to_pydict() == {
        "name": ["row-6", "row-7"]
    }


@pytest.mark.integration()
@pytest.mark.parametrize("kind", ["file", "directory", "glob", "list"])
def test_read_orc_s3(minio_io_config: daft.io.IOConfig, kind: str) -> None:
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket):
        root = f"s3://{bucket}/orc"
        first = f"{root}/first.orc"
        second = f"{root}/nested/second.orc"
        fs.write_bytes(first, _orc_bytes(pa.table({"id": [1, 2], "name": ["a", "b"]})))
        fs.write_bytes(second, _orc_bytes(pa.table({"id": [3, 4], "name": ["c", "d"]})))
        fs.write_bytes(f"{root}/unrelated.txt", b"not ORC")
        paths = {"file": first, "directory": root, "glob": f"{root}/**/*.orc", "list": [first, second]}
        df = daft.read_orc(paths[kind], io_config=minio_io_config, batch_size=1)
        expected = (
            {"id": [1, 2], "name": ["a", "b"]} if kind == "file" else {"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"]}
        )
        assert df.sort("id").to_pydict() == expected


@pytest.mark.integration()
def test_read_orc_s3_default_io_config(minio_io_config: daft.io.IOConfig) -> None:
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket):
        path = f"s3://{bucket}/data"
        fs.write_bytes(path, _orc_bytes(pa.table({"id": [1, 2]})))
        with daft.context.planning_config_ctx(default_io_config=minio_io_config):
            df = daft.read_orc(path, batch_size=1)
        # Tasks must retain the configuration captured during planning.
        assert df.to_pydict() == {"id": [1, 2]}


@pytest.mark.integration()
def test_read_orc_s3_multiple_stripes(minio_io_config: daft.io.IOConfig, tmp_path: Path) -> None:
    table = pa.table({"id": range(12000), "name": [f"row-{i:08d}-" + "x" * 80 for i in range(12000)]})
    data = _orc_bytes(table)
    assert orc.ORCFile(io.BytesIO(data)).nstripes > 1
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket):
        path = f"s3://{bucket}/stripes.orc"
        fs.write_bytes(path, data)
        df = daft.read_orc(path, io_config=minio_io_config, batch_size=257)
        assert df.count_rows() == table.num_rows
        assert df.to_pydict() == table.to_pydict()
        output = tmp_path / "parquet"
        df.where(daft.col("id") >= 11997).select("id", "name").write_parquet(str(output)).collect()
        assert daft.read_parquet(str(output)).sort("id").to_pydict() == table.slice(11997).to_pydict()


@pytest.mark.integration()
def test_read_orc_s3_schema_alignment(minio_io_config: daft.io.IOConfig) -> None:
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket):
        first = f"s3://{bucket}/first.orc"
        second = f"s3://{bucket}/second.orc"
        fs.write_bytes(first, _orc_bytes(pa.table({"id": [1], "name": ["a"]})))
        fs.write_bytes(second, _orc_bytes(pa.table({"id": [2, 3]})))
        df = daft.read_orc([first, second], io_config=minio_io_config, batch_size=1)
        assert df.sort("id").to_pydict() == {"id": [1, 2, 3], "name": ["a", None, None]}
        assert df.where(daft.col("name").is_null()).select("id").sort("id").to_pydict() == {"id": [2, 3]}


@pytest.mark.integration()
def test_read_orc_s3_corrupt_input(minio_io_config: daft.io.IOConfig) -> None:
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket):
        path = f"s3://{bucket}/bad.orc"
        fs.write_bytes(path, b"not ORC")
        with pytest.raises(OSError, match="(?i)orc"):
            daft.read_orc(path, io_config=minio_io_config)
