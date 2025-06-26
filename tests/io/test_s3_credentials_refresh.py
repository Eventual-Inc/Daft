from __future__ import annotations

import datetime
import os
import time
import uuid

import boto3

import daft
import daft.context
from tests.conftest import get_tests_daft_runner_name


def test_s3_credentials_refresh(aws_server, aws_server_ip, aws_server_port, aws_credentials):
    server_url = f"http://{aws_server_ip}:{aws_server_port}"

    bucket_name = "mybucket-" + str(uuid.uuid4())
    input_file_path = f"s3://{bucket_name}/input.parquet"
    output_file_path = f"s3://{bucket_name}/output.parquet"

    old_env = os.environ.copy()
    # Set required AWS environment variables before starting server.
    # Required to opt out of concurrent writing, since we don't provide a LockClient.
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"

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
            expiry=(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=2)),
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
    df.write_parquet(input_file_path, io_config=static_config)

    df = daft.read_parquet(input_file_path, io_config=dynamic_config)
    assert count_get_credentials == 1

    df.collect()
    assert count_get_credentials == 1

    # the credential is still active
    df = daft.read_parquet(input_file_path, io_config=dynamic_config)
    assert count_get_credentials == 1

    # the credential is expired after 2s, it will be refreshed in next time.
    pre_count = count_get_credentials
    time.sleep(2)
    df.collect()
    assert count_get_credentials > pre_count

    # The credential is expired again after 2s
    pre_count = count_get_credentials
    time.sleep(2)
    df.write_parquet(output_file_path, io_config=dynamic_config)

    is_ray_runner = (
        get_tests_daft_runner_name() == "ray"
    )  # hack because ray runner will not increment `count_get_credentials`
    assert count_get_credentials > pre_count or is_ray_runner

    df2 = daft.read_parquet(output_file_path, io_config=static_config)
    assert df.to_arrow() == df2.to_arrow()

    # Restore old set of environment variables.
    os.environ = old_env
