from __future__ import annotations

import io

import boto3
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as papq
import pytest

import daft


def daft_read(path: str, columns: list[str] | None = None) -> pa.Table:
    df = daft.read_parquet(path)
    if columns is not None:
        df = df.select(*columns)
    return df.to_arrow()


def pyarrow_read(path: str, columns: list[str] | None = None) -> pa.Table:
    fs = None
    if path.startswith("s3://"):
        fs = pafs.S3FileSystem()
        path = path.replace("s3://", "")
    return papq.read_table(path, columns=columns, filesystem=fs)


def boto3_get_object_read(path: str, columns: list[str] | None = None) -> pa.Table:
    if path.startswith("s3://"):
        client = boto3.client("s3", region_name="us-west-2")
        split_path = path.replace("s3://", "").split("/")
        bucket, key = split_path[0], path.replace(f"s3://{split_path[0]}/", "")
        resp = client.get_object(Bucket=bucket, Key=key)
        data = io.BytesIO(resp["Body"].read())
        tbl = papq.read_table(data, columns=columns)
        return tbl

    with open(path, "rb") as f:
        return papq.read_table(f, columns=columns)


@pytest.fixture(params=[daft_read, pyarrow_read, boto3_get_object_read], ids=["daft", "pyarrow", "boto3_get_object"])
def read_fn(request):
    """Fixture which returns the function to read a PyArrow table from a path"""
    return request.param
