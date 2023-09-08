from __future__ import annotations

import uuid

import pytest

from daft.table import Table

BUCKETS = ["head-retries-parquet-bucket", "get-retries-parquet-bucket"]


@pytest.mark.integration()
@pytest.mark.parametrize("status_code", [(400, "foo"), (403, "foo"), (404, "foo")])
@pytest.mark.parametrize("bucket", BUCKETS)
def test_non_retryable_errors(retry_server_s3_config, status_code: tuple[int, str], bucket: str):
    status_code, status_code_str = status_code
    data_path = f"s3://{bucket}/{status_code}/{status_code_str}/1/{uuid.uuid4()}"
    with pytest.raises((FileNotFoundError, ValueError)):
        Table.read_parquet(data_path, io_config=retry_server_s3_config)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "status_code",
    [
        # NOTE: Certain errors are only correctly retried when the correct str code is returned in the XML response
        # For these cases, we can pass them into our custom S3 endpoint so that it will echo it in the XML response on return
        (500, "InternalError"),
        (503, "SlowDown"),
        (504, "PLACEHOLDER_CODE - should not matter"),
        # TODO: [IO-RETRIES] We should also retry correctly on these error codes
        # These are marked as retryable error codes, see:
        # https://github.com/aws/aws-sdk-cpp/blob/8a9550f1db04b33b3606602ba181d68377f763df/src/aws-cpp-sdk-core/include/aws/core/http/HttpResponse.h#L113-L131
        # 509,
        # 408,
        # 429,
        # 419,
        # 440,
        # 598,
        # 599,
    ],
)
@pytest.mark.parametrize("bucket", BUCKETS)
def test_retryable_errors(retry_server_s3_config, status_code: tuple[int, str], bucket: str):
    # By default the SDK retries 3 times, so we should be able to tolerate NUM_ERRORS=2
    # Tweak this variable appropriately to match the retry policy
    NUM_ERRORS = 2
    status_code, status_code_str = status_code
    data_path = f"s3://{bucket}/{status_code}/{status_code_str}/{NUM_ERRORS}/{uuid.uuid4()}"

    Table.read_parquet(data_path, io_config=retry_server_s3_config)
