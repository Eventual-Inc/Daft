from __future__ import annotations

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


@pytest.fixture(params=[daft_read, pyarrow_read], ids=["daft", "pyarrow"])
def read_fn(request):
    """Fixture which returns the function to read a PyArrow table from a path"""
    return request.param
