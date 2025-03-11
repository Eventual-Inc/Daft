import pytest
import boto3
import os

from daft import Catalog
from typing import TYPE_CHECKING
from moto import mock_aws

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
else:
    S3Client = object

import pytest


# https://github.com/Eventual-Inc/Daft/issues/3925
@pytest.mark.skip("S3 Tables will required integration tests, for now verify manually.")
def test_sanity():
    table_bucket_arn = "placeholder"
    s3tb = Catalog.from_s3tables(table_bucket_arn)
    s3tb.read_table("demo.points").show()

@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")

@pytest.fixture(scope="function")
def mocked_aws(aws_credentials):
    with mock_aws():
        yield

def test_s3_bucket_creation(s3: S3Client):
    s3.create_bucket(Bucket="somebucket")

    result = s3.list_buckets()
    print(result)
    assert len(result["Buckets"]) == 1
