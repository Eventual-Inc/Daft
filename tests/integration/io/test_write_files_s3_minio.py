from __future__ import annotations

import uuid

import pytest
import s3fs

import daft
import daft.filesystem as fs_mod
from daft.context import planning_config_ctx
from daft.io import IOConfig
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
    reason="Endpoint environment changes are local to the native runner process",
)
@pytest.mark.parametrize("protocol", ["s3://", "s3a://", "s3n://"])
@pytest.mark.parametrize("file_format", ["json", "csv", "parquet"])
@pytest.mark.parametrize("write_mode", ["overwrite", "overwrite-partitions"])
@pytest.mark.parametrize("endpoint_variable", ["AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"])
def test_writing_overwrite_endpoint_from_environment(
    minio_io_config, bucket, protocol, file_format, write_mode, endpoint_variable, monkeypatch
):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    monkeypatch.delenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", raising=False)
    monkeypatch.setenv(endpoint_variable, minio_io_config.s3.endpoint_url)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", minio_io_config.s3.key_id)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", minio_io_config.s3.access_key)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setattr(fs_mod, "_CACHED_FSES", {})
    path = f"{protocol}{bucket}/overwrite-env-{uuid.uuid4()}"

    with planning_config_ctx(default_io_config=IOConfig()):
        initial = daft.from_pydict({"value": [1, 2], "part": ["a", "b"]})
        getattr(initial, f"write_{file_format}")(path, partition_cols=["part"])
        replacement = daft.from_pydict({"value": [3], "part": ["a"]})
        getattr(replacement, f"write_{file_format}")(path, partition_cols=["part"], write_mode=write_mode)
        actual = getattr(daft, f"read_{file_format}")(f"{path}/**/*.{file_format}").select("value").to_pydict()

    expected = [3] if write_mode == "overwrite" else [2, 3]
    assert sorted(actual["value"]) == expected
