from __future__ import annotations

import uuid

import pytest
import s3fs

import daft


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
def test_writing_parquet(minio_io_config, bucket):
    data = {
        "foo": [1, 2, 3],
        "bar": ["a", "b", "c"],
    }
    df = daft.from_pydict(data)
    df = df.repartition(2)
    results = df.write_parquet(
        f"s3://{bucket}/parquet-writes-{uuid.uuid4()}",
        partition_cols=["bar"],
        io_config=minio_io_config,
    )
    results.collect()
    assert len(results) == 3


@pytest.mark.integration()
def test_writing_parquet_overwrite(minio_io_config, bucket):
    data = {
        "foo": [1, 2, 3],
        "bar": ["a", "b", "c"],
    }
    df = daft.from_pydict(data)
    df = df.repartition(2)

    path = f"s3://{bucket}/parquet-writes-{uuid.uuid4()}"

    # first write
    results = df.write_parquet(
        path,
        io_config=minio_io_config,
    )
    results.collect()
    assert len(results) == 2

    # second write
    results2 = df.write_parquet(
        path,
        io_config=minio_io_config,
    )
    results2.collect()
    assert len(results2) == 2

    # read, data should have been overwritten
    df_read = daft.read_parquet(path, io_config=minio_io_config).collect()
    print(df_read)
    assert len(df_read) == 3

    pydict = df_read.to_pydict()
    assert set(pydict["foo"]) == {1, 2, 3}
    assert set(pydict["bar"]) == {"a", "b", "c"}


@pytest.mark.integration()
def test_writing_parquet_overwrite_partitions(minio_io_config, bucket):
    data = {
        "foo": [1, 2, 3],
        "bar": ["a", "b", "c"],
    }
    df = daft.from_pydict(data)
    df = df.repartition(2)

    path = f"s3://{bucket}/parquet-writes-{uuid.uuid4()}"

    # first write
    results = df.write_parquet(
        path,
        partition_cols=["bar"],
        io_config=minio_io_config,
    )
    results.collect()
    assert len(results) == 3

    data2 = {
        "foo": [4, 5],
        "bar": ["a", "b"],
    }
    df2 = daft.from_pydict(data2)
    df2 = df2.repartition(2)

    # second write only overwrites two partitions
    results2 = df2.write_parquet(
        path,
        partition_cols=["bar"],
        io_config=minio_io_config,
    )
    results2.collect()
    assert len(results2) == 2

    # read, data from two partitions should have been overwritten
    paths = [f"{path}/bar={bar}" for bar in ["a", "b", "c"]]
    df_read = daft.read_parquet(paths, io_config=minio_io_config).collect()
    print(df_read)
    assert len(df_read) == 3

    pydict = df_read.to_pydict()
    assert set(pydict["foo"]) == {3, 4, 5}
    assert set(pydict["bar"]) == {"a", "b", "c"}
