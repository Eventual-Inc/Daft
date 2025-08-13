from __future__ import annotations

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
    BUCKET = "my-bucket"

    fs = s3fs.S3FileSystem(
        key=minio_io_config.s3.key_id,
        password=minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": minio_io_config.s3.endpoint_url},
    )
    if not fs.exists(BUCKET):
        fs.mkdir(BUCKET)
    yield BUCKET


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
    bucket_name = "my-public-bucket"
    with minio_create_public_bucket(minio_io_config=minio_io_config, bucket_name=bucket_name):
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
@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
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
