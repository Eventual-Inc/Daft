from __future__ import annotations

import datetime
import io
import os
import time
from collections.abc import Iterator

import boto3
import pytest

import daft
from tests.io.mock_aws_server import start_service, stop_process


@pytest.fixture(scope="session")
def aws_log_file(tmp_path_factory: pytest.TempPathFactory) -> Iterator[io.IOBase]:
    # NOTE(Clark): We have to use a log file for the mock AWS server's stdout/sterr.
    # - If we use None, then the server output will spam stdout.
    # - If we use PIPE, then the server will deadlock if the (relatively small) buffer fills, and the server is pretty
    #   noisy.
    # - If we use DEVNULL, all log output is lost.
    # With a tmp_path log file, we can prevent spam and deadlocks while also providing an avenue for debuggability, via
    # changing this fixture to something persistent, or dumping the file to stdout before closing the file, etc.
    tmp_path = tmp_path_factory.mktemp("aws_logging")
    with open(tmp_path / "aws_log.txt", "w") as f:
        yield f


def test_s3_credentials_refresh(aws_log_file: io.IOBase):
    host = "127.0.0.1"
    port = 5000

    server_url = f"http://{host}:{port}"

    bucket_name = "mybucket"
    file_name = "test.parquet"

    s3_file_path = f"s3://{bucket_name}/{file_name}"

    old_env = os.environ.copy()
    # Set required AWS environment variables before starting server.
    # Required to opt out of concurrent writing, since we don't provide a LockClient.
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"

    # Start moto server.
    process = start_service(host, port, aws_log_file)

    aws_credentials = {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
    }

    s3 = boto3.resource(
        "s3",
        region_name="us-west-2",
        use_ssl=False,
        endpoint_url=server_url,
        aws_access_key_id=aws_credentials["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=aws_credentials["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=aws_credentials["AWS_SESSION_TOKEN"],
    )
    bucket = s3.Bucket(bucket_name)
    bucket.create(CreateBucketConfiguration={"LocationConstraint": "us-west-2"})

    count_get_credentials = 0

    def get_credentials():
        nonlocal count_get_credentials
        count_get_credentials += 1
        return daft.io.S3Credentials(
            key_id=aws_credentials["AWS_ACCESS_KEY_ID"],
            access_key=aws_credentials["AWS_SECRET_ACCESS_KEY"],
            session_token=aws_credentials["AWS_SESSION_TOKEN"],
            expiry=(datetime.datetime.now() + datetime.timedelta(seconds=1)),
        )

    static_config = daft.io.IOConfig(
        s3=daft.io.S3Config(
            endpoint_url=server_url,
            region_name="us-west-2",
            key_id=aws_credentials["AWS_ACCESS_KEY_ID"],
            access_key=aws_credentials["AWS_SECRET_ACCESS_KEY"],
            session_token=aws_credentials["AWS_SESSION_TOKEN"],
            use_ssl=False,
        )
    )

    dynamic_config = daft.io.IOConfig(
        s3=daft.io.S3Config(
            endpoint_url=server_url,
            region_name="us-west-2",
            credentials_provider=get_credentials,
            buffer_time=0,
            use_ssl=False,
        )
    )

    df = daft.from_pydict({"a": [1, 2, 3]})
    df.write_parquet(s3_file_path, io_config=static_config)

    df = daft.read_parquet(s3_file_path, io_config=dynamic_config)
    assert count_get_credentials == 1

    df.collect()
    assert count_get_credentials == 1

    df = daft.read_parquet(s3_file_path, io_config=dynamic_config)
    assert count_get_credentials == 1

    time.sleep(1)
    df.collect()
    assert count_get_credentials == 2

    # Shutdown moto server.
    stop_process(process)
    # Restore old set of environment variables.
    os.environ = old_env
