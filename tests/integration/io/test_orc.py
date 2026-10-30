from __future__ import annotations

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
