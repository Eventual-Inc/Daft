from __future__ import annotations

import io

import boto3
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as papq
import pytest

import daft

# def daft_legacy_read(path: str, columns: list[str] | None = None) -> pa.Table:
#     df = daft.read_parquet(path)
#     if columns is not None:
#         df = df.select(*columns)
#     return df.to_arrow()


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
        data = io.BytesIO(f.read())
        return papq.read_table(data, columns=columns)


def daft_native_read(path: str, columns: list[str] | None = None) -> pa.Table:
    tbl = daft.table.Table.read_parquet(path, columns=columns)
    return tbl.to_arrow()


def daft_native_read_to_arrow(path: str, columns: list[str] | None = None) -> pa.Table:
    return daft.table.read_parquet_into_pyarrow(path, columns=columns)


@pytest.fixture(
    params=[
        daft_native_read,
        daft_native_read_to_arrow,
        pyarrow_read,
        boto3_get_object_read,
    ],
    ids=[
        "daft_native_read",
        "daft_native_read_to_arrow",
        "pyarrow",
        "boto3_get_object",
    ],
)
def read_fn(request):
    """Fixture which returns the function to read a PyArrow table from a path"""
    return request.param


def bulk_read_adapter(func):
    def fn(files: list[str]) -> list[pa.Table]:
        return [func(f) for f in files]

    return fn


def daft_bulk_read(paths: list[str], columns: list[str] | None = None) -> list[pa.Table]:
    tables = daft.table.Table.read_parquet_bulk(paths, columns=columns)
    return [t.to_arrow() for t in tables]


def daft_into_pyarrow_bulk_read(paths: list[str], columns: list[str] | None = None) -> list[pa.Table]:
    return daft.table.read_parquet_into_pyarrow_bulk(paths, columns=columns)


def pyarrow_bulk_read(paths: list[str], columns: list[str] | None = None) -> list[pa.Table]:
    return [pyarrow_read(f, columns=columns) for f in paths]


def boto_bulk_read(paths: list[str], columns: list[str] | None = None) -> list[pa.Table]:
    return [boto3_get_object_read(f, columns=columns) for f in paths]


@pytest.fixture(
    params=[
        daft_bulk_read,
        daft_into_pyarrow_bulk_read,
        pyarrow_bulk_read,
        boto_bulk_read,
    ],
    ids=[
        "daft_bulk_read",
        "daft_into_pyarrow_bulk_read",
        "pyarrow_bulk_read",
        "boto3_bulk_read",
    ],
)
def bulk_read_fn(request):
    """Fixture which returns the function to read a PyArrow table from a path"""
    return request.param
