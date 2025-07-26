from __future__ import annotations

import json
import uuid

import boto3
import pytest
import s3fs
from botocore.config import Config

import daft
from tests.conftest import get_tests_daft_runner_name


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


@pytest.fixture(scope="function")
def anonymous_bucket(minio_io_config):
    # For some reason s3fs is having trouble cleaning up MinIO
    # folders created by pyarrow write_parquet. We just write to
    # paths with random UUIDs to work around this.
    BUCKET = "my-bucket-anonymous"

    # Create authenticated S3 client to set up the bucket.
    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_io_config.s3.endpoint_url,
        aws_access_key_id=minio_io_config.s3.key_id,
        aws_secret_access_key=minio_io_config.s3.access_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # Create bucket if it doesn't exist.
    try:
        s3_client.create_bucket(Bucket=BUCKET)
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass

    # Set bucket policy for anonymous access.
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                "Resource": [f"arn:aws:s3:::{BUCKET}", f"arn:aws:s3:::{BUCKET}/*"],
            }
        ],
    }
    s3_client.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(bucket_policy))

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
def test_writing_parquet_anonymous_mode(anonymous_minio_io_config, anonymous_bucket, protocol):
    data = {
        "foo": [1, 2, 3],
        "bar": ["a", "b", "c"],
    }
    df = daft.from_pydict(data)
    df = df.repartition(2)
    results = df.write_parquet(
        f"{protocol}{anonymous_bucket}/parquet-writes-{uuid.uuid4()}",
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
