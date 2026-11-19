from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import uuid

import pytest
import s3fs

import daft
from tests.conftest import get_tests_daft_runner_name, minio_create_public_bucket


@pytest.fixture(scope="function")
def bucket(minio_io_config):
    # For some reason s3fs is having trouble cleaning up MinIO
    # folders created by pyarrow write_parquet. We just write to
    # paths with random UUIDs to work around this.
    bucket_name = f"bucket-{uuid.uuid4()}"

    fs = s3fs.S3FileSystem(
        key=minio_io_config.s3.key_id,
        password=minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": minio_io_config.s3.endpoint_url},
    )
    if not fs.exists(bucket_name):
        fs.mkdir(bucket_name)
    try:
        yield bucket_name
    finally:
        try:
            fs.rm(bucket_name, recursive=True)
        except FileNotFoundError:
            # Bucket may have already been deleted, which is fine
            pass


@pytest.mark.integration()
@pytest.mark.parametrize("protocol", ["s3://", "s3a://", "s3n://"])
def test_writing_parquet(minio_io_config, bucket, protocol):
    data = {
        "foo": [1, 2, 3],
        "bar": ["a", "b", "c"],
    }
    df = daft.from_pydict(data)
    df = df.repartition(2)
    results = df.write_parquet(
        f"{protocol}{bucket}/parquet-writes-{uuid.uuid4()}",
        partition_cols=["bar"],
        io_config=minio_io_config,
    )
    results.collect()
    assert len(results) == 3


@pytest.mark.integration()
@pytest.mark.parametrize("protocol", ["s3://", "s3a://", "s3n://"])
def test_writing_parquet_anonymous_mode(anonymous_minio_io_config, minio_io_config, protocol):
    with minio_create_public_bucket(minio_io_config=minio_io_config) as bucket_name:
        data = {
            "foo": [1, 2, 3],
            "bar": ["a", "b", "c"],
        }
        df = daft.from_pydict(data)
        df = df.repartition(2)
        results = df.write_parquet(
            f"{protocol}{bucket_name}/parquet-writes-{uuid.uuid4()}",
            partition_cols=["bar"],
            io_config=anonymous_minio_io_config,
        )
        results.collect()
        assert len(results) == 3


@pytest.mark.integration()
@pytest.mark.parametrize("protocol", ["s3://", "s3a://", "s3n://"])
def test_writing_json(minio_io_config, bucket, protocol):
    data = {
        "foo": [1, 2, 3],
        "bar": ["a", "b", "c"],
    }
    df = daft.from_pydict(data)
    df = df.repartition(2)
    results = df.write_json(
        f"{protocol}{bucket}/json-writes-{uuid.uuid4()}",
        partition_cols=["bar"],
        io_config=minio_io_config,
    )
    results.collect()
    assert len(results) == 3


@pytest.mark.integration()
@pytest.mark.timeout(60)
@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native",
    reason="The subprocess regression explicitly exercises the native runner",
)
@pytest.mark.parametrize("protocol", ["s3://", "s3a://", "s3n://"])
@pytest.mark.parametrize(
    "write_mode,partitioned",
    [("append", False), ("append", True), ("overwrite", False), ("overwrite", True), ("overwrite-partitions", True)],
)
@pytest.mark.parametrize("endpoint_variable", ["AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"])
def test_writing_pyarrow_parquet_endpoint_from_environment(
    minio_io_config, bucket, protocol, write_mode, partitioned, endpoint_variable
):
    # Native IO clients cache environment-derived configuration. A fresh process
    # both exercises first-use resolution and prevents leaking a MinIO client
    # into later tests that use the default IOConfig.
    env = os.environ.copy()
    for name in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "AWS_SESSION_TOKEN"):
        env.pop(name, None)
    env.update(
        {
            endpoint_variable: minio_io_config.s3.endpoint_url,
            "AWS_ACCESS_KEY_ID": minio_io_config.s3.key_id,
            "AWS_SECRET_ACCESS_KEY": minio_io_config.s3.access_key,
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_REGION": "us-east-1",
            "DAFT_RUNNER": "native",
        }
    )
    path = f"{protocol}{bucket}/pyarrow-env-{uuid.uuid4()}"
    script = textwrap.dedent("""
        import os
        import sys

        import pyarrow.fs as pafs
        import pyarrow.parquet as pq

        import daft
        from daft.context import execution_config_ctx

        path, write_mode, partitioned = sys.argv[1:]
        partition_cols = ["part"] if partitioned == "True" else None
        with execution_config_ctx(native_parquet_writer=False):
            initial = daft.from_pydict({"value": [1, 2], "part": ["a", "b"]})
            initial.write_parquet(path, partition_cols=partition_cols)
            replacement = daft.from_pydict({"value": [3], "part": ["a"]})
            replacement.write_parquet(path, partition_cols=partition_cols, write_mode=write_mode)
        expected = {"append": [1, 2, 3], "overwrite": [3], "overwrite-partitions": [2, 3]}[write_mode]
        external_fs = pafs.S3FileSystem(
            endpoint_override=os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ["AWS_ENDPOINT_URL"],
            access_key=os.environ["AWS_ACCESS_KEY_ID"],
            secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region=os.environ["AWS_DEFAULT_REGION"],
        )
        external = pq.read_table(
            path.split("://", 1)[1], filesystem=external_fs, columns=["value"], partitioning=None
        ).to_pydict()
        assert sorted(external["value"]) == expected, external
        actual = daft.read_parquet(f"{path}/**/*.parquet").select("value").to_pydict()
        assert sorted(actual["value"]) == expected, actual
    """)
    result = subprocess.run(
        [sys.executable, "-c", script, path, write_mode, str(partitioned)],
        env=env,
        capture_output=True,
        text=True,
        timeout=50,
    )
    assert result.returncode == 0, result.stdout + result.stderr
