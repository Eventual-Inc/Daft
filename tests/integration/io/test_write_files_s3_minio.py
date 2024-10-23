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


def write(
    df: daft.DataFrame,
    path: str,
    format: str,
    write_mode: str,
    partition_col: str | None = None,
    io_config: daft.io.IOConfig | None = None,
):
    if format == "parquet":
        return df.write_parquet(
            path,
            write_mode=write_mode,
            partition_cols=[partition_col],
            io_config=io_config,
        )
    elif format == "csv":
        return df.write_csv(
            path,
            write_mode=write_mode,
            partition_cols=[partition_col],
            io_config=io_config,
        )
    else:
        raise ValueError(f"Unsupported format: {format}")


def read(path: str, format: str, io_config: daft.io.IOConfig | None = None):
    if format == "parquet":
        return daft.read_parquet(path, io_config=io_config)
    elif format == "csv":
        return daft.read_csv(path, io_config=io_config)
    else:
        raise ValueError(f"Unsupported format: {format}")


@pytest.mark.integration()
@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("num_partitions", [1, 2])
@pytest.mark.parametrize("partition_cols", [None, ["a"]])
@pytest.mark.parametrize("protocol", ["s3://", "s3a://"])
def test_write_modes_s3_minio(
    minio_io_config,
    bucket,
    protocol,
    write_mode,
    format,
    num_partitions,
    partition_cols,
):
    path = f"{protocol}{bucket}/write_modes_s3_minio-{uuid.uuid4()}"
    existing_data = {"a": [i for i in range(10)]}
    # Write some existing_data
    write(
        daft.from_pydict(existing_data).into_partitions(num_partitions),
        path,
        format,
        "append",
        partition_cols,
        minio_io_config,
    )

    # Write some new data
    new_data = {
        "a": [i for i in range(10, 20)],
    }
    write(
        daft.from_pydict(new_data).into_partitions(num_partitions),
        path,
        format,
        write_mode,
        partition_cols,
        minio_io_config,
    )

    # Read back the data
    read_path = path + "/**" if partition_cols is not None else path
    read_back = read(read_path, format, minio_io_config).sort("a").to_pydict()

    # Check the data
    if write_mode == "append":
        assert read_back["a"] == existing_data["a"] + new_data["a"]
    elif write_mode == "overwrite":
        assert read_back["a"] == new_data["a"]
    else:
        raise ValueError(f"Unsupported write_mode: {write_mode}")
