from __future__ import annotations

import io
import os
import subprocess
import sys
import textwrap
import threading
from collections.abc import Iterator
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

import pyarrow as pa
import pytest
from pyarrow import orc

import daft
from daft.exceptions import MiscTransientError
from tests.conftest import get_tests_daft_runner_name
from tests.integration.io.conftest import minio_create_bucket
from tests.io.test_orc import _corrupt_orc_stripe


@pytest.fixture
def minio_io_config(minio_io_config: daft.io.IOConfig) -> daft.io.IOConfig:
    endpoint = os.getenv("DAFT_ORC_TEST_S3_ENDPOINT")
    if endpoint is None:
        return minio_io_config
    return daft.io.IOConfig(s3=minio_io_config.s3.replace(endpoint_url=endpoint))


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


@pytest.mark.integration()
def test_orc_http_ignore_corrupt_report(orc_http_url: str, tmp_path: Path) -> None:
    (tmp_path / "bad.orc").write_bytes(b"not ORC")
    bad_url = orc_http_url.rsplit("/", 1)[0] + "/bad.orc"
    df = daft.read_orc([bad_url, orc_http_url], batch_size=2, ignore_corrupt_files=True).collect()
    assert df.to_pydict()["id"] == list(range(9))
    assert len(df.skipped_corrupt_files) == 1
    path, reason, partial = df.skipped_corrupt_files[0]
    assert path == bad_url and "Not an ORC file" in reason and not partial


@pytest.mark.integration()
@pytest.mark.parametrize("ignore", [False, True])
@pytest.mark.parametrize("status", [403, 503])
@pytest.mark.parametrize("phase", ["inference", "execution"])
def test_orc_http_io_errors_propagate(tmp_path: Path, ignore: bool, status: int, phase: str) -> None:
    (tmp_path / "data.orc").write_bytes(_orc_bytes(pa.table({"id": [1, 2]})))
    failing = phase == "inference"
    failed_gets: list[int] = []

    class Handler(_QuietHTTPRequestHandler):
        def do_GET(self) -> None:
            if failing:
                failed_gets.append(status)
                self.send_error(status)
            else:
                super().do_GET()

        def do_HEAD(self) -> None:
            # Keep discovery successful so GET failures exercise the ORC
            # inference/decoder boundary rather than only path enumeration.
            super().do_HEAD()

    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(Handler, directory=str(tmp_path)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = daft.io.IOConfig(http=daft.io.HTTPConfig(num_tries=1, read_timeout_ms=1000, connect_timeout_ms=1000))
    try:
        path = f"http://127.0.0.1:{server.server_port}/data.orc"
        reason = "Unable to open file" if status == 403 else "Misc Transient error trying to read path"
        with pytest.raises(Exception, match=f"Get failed: {reason}"):
            df = daft.read_orc(path, io_config=config, ignore_corrupt_files=ignore)
            failing = True
            df.collect()
        assert failed_gets and all(code == status for code in failed_gets)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.integration()
def test_orc_s3_ignore_corrupt_complete_report(minio_io_config: daft.io.IOConfig) -> None:
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket):
        root = f"s3://{bucket}"
        good = f"{root}/good.orc"
        bad = [f"{root}/bad1.orc", f"{root}/bad2.orc"]
        fs.write_bytes(good, _orc_bytes(pa.table({"id": [1, 2]})))
        for path in bad:
            fs.write_bytes(path, b"not ORC")
        df = daft.read_orc([*bad, good], io_config=minio_io_config, ignore_corrupt_files=True).collect()
        assert df.to_pydict() == {"id": [1, 2]}
        assert {path for path, _, _ in df.skipped_corrupt_files} == set(bad)
        assert len(df.skipped_corrupt_files) == 2
        assert all(reason and not partial for _, reason, partial in df.skipped_corrupt_files)


@pytest.mark.integration()
def test_orc_s3_ignore_corrupt_partial(minio_io_config: daft.io.IOConfig) -> None:
    table = pa.table({"id": range(12000), "name": [f"row-{i:08d}-" + "x" * 80 for i in range(12000)]})
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket):
        path = f"s3://{bucket}/partial.orc"
        fs.write_bytes(path, _corrupt_orc_stripe(_orc_bytes(table)))
        df = daft.read_orc(path, io_config=minio_io_config, batch_size=257, ignore_corrupt_files=True).collect()
        result = df.to_pydict()
        assert 0 < len(result["id"]) < table.num_rows
        assert result == table.slice(0, len(result["id"])).to_pydict()
        assert len(df.skipped_corrupt_files) == 1
        reported, reason, partial = df.skipped_corrupt_files[0]
        assert reported == path and reason and partial


@pytest.mark.integration()
@pytest.mark.parametrize("ignore", [False, True])
def test_orc_s3_permissions_propagate(minio_io_config: daft.io.IOConfig, ignore: bool) -> None:
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket):
        path = f"s3://{bucket}/private.orc"
        fs.write_bytes(path, _orc_bytes(pa.table({"id": [1]})))
        with pytest.raises(HTTPError) as denied:
            urlopen(f"{minio_io_config.s3.endpoint_url}/{bucket}/private.orc", timeout=5)
        assert denied.value.code == 403
        denied.value.close()
        anonymous = daft.io.IOConfig(
            s3=daft.io.S3Config(
                endpoint_url=minio_io_config.s3.endpoint_url, use_ssl=False, anonymous=True, num_tries=1
            )
        )
        # SeaweedFS's anonymous rejection is exposed by the existing S3 client
        # as MiscTransientError. The error must propagate for both flag values.
        with pytest.raises(MiscTransientError, match="trying to read path"):
            daft.read_orc(path, io_config=anonymous, ignore_corrupt_files=ignore).collect()


@pytest.mark.integration()
def test_orc_multiple_worker_complete_report(tmp_path: Path) -> None:
    paths = []
    expected_bad = set()
    for i in range(32):
        path = tmp_path / f"input-{i}.orc"
        if i % 2:
            path.write_bytes(b"not ORC")
            expected_bad.add(path.name)
        else:
            orc.write_table(pa.table({"id": [i] * 1024}), path)
        paths.append(str(path))
    if get_tests_daft_runner_name() == "native":
        # The existing synchronous Python bridge can occupy all eight I/O
        # threads at default scan concurrency. Leave threads for native file I/O.
        with daft.execution_config_ctx(scantask_max_parallel=2):
            df = daft.read_orc(paths, ignore_corrupt_files=True).collect()
        assert len(df.to_pydict()["id"]) == 16 * 1024
        assert {Path(path).name for path, _, _ in df.skipped_corrupt_files} == expected_bad
        assert len(df.skipped_corrupt_files) == 16
        return

    # Each Daft Ray worker serves one Ray node. A separate process isolates this
    # two-node cluster from the already initialized pytest runner.
    code = textwrap.dedent(
        """
        import sys
        from pathlib import Path
        import ray
        from ray.cluster_utils import Cluster
        import daft
        cluster = Cluster()
        try:
            cluster.add_node(num_cpus=1, memory=512 * 1024**2, object_store_memory=90 * 1024**2, include_dashboard=False)
            cluster.add_node(num_cpus=1, memory=512 * 1024**2, object_store_memory=90 * 1024**2)
            ray.init(address=cluster.address, logging_level='ERROR')
            daft.set_runner_ray(noop_if_initialized=True)
            daft.set_execution_config(scantask_max_parallel=2)
            paths = [str(Path(sys.argv[1]) / f'input-{i}.orc') for i in range(32)]
            df = daft.read_orc(paths, ignore_corrupt_files=True).collect()
            assert len(df.to_pydict()['id']) == 16 * 1024
            assert {Path(path).name for path, _, _ in df.skipped_corrupt_files} == {f'input-{i}.orc' for i in range(1,32,2)}
            assert len(df.skipped_corrupt_files) == 16
            assert all(reason and not partial for _, reason, partial in df.skipped_corrupt_files)
            workers = [a for a in ray._private.state.actors().values() if a.get('ActorClassName') == 'RaySwordfishActor' and a['State'] == 'ALIVE']
            assert len(workers) == 2, workers
            print('two Ray workers; all 16 corrupt files reported')
        finally:
            ray.shutdown()
            cluster.shutdown()
        """
    )
    result = subprocess.run([sys.executable, "-c", code, str(tmp_path)], capture_output=True, text=True, timeout=180)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "two Ray workers; all 16 corrupt files reported" in result.stdout
