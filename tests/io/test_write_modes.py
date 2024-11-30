import uuid
from typing import List, Optional

import pytest
import s3fs

import daft


def write(
    df: daft.DataFrame,
    path: str,
    format: str,
    write_mode: str,
    partition_cols: Optional[List[str]] = None,
    io_config: Optional[daft.io.IOConfig] = None,
):
    if format == "parquet":
        return df.write_parquet(
            path,
            write_mode=write_mode,
            partition_cols=partition_cols,
            io_config=io_config,
        )
    elif format == "csv":
        return df.write_csv(
            path,
            write_mode=write_mode,
            partition_cols=partition_cols,
            io_config=io_config,
        )
    else:
        raise ValueError(f"Unsupported format: {format}")


def read(path: str, format: str, io_config: Optional[daft.io.IOConfig] = None):
    if format == "parquet":
        return daft.read_parquet(path, io_config=io_config)
    elif format == "csv":
        return daft.read_csv(path, io_config=io_config)
    else:
        raise ValueError(f"Unsupported format: {format}")


def arrange_write_mode_test(existing_data, new_data, path, format, write_mode, partition_cols, io_config):
    # Write some existing_data
    write(existing_data, path, format, "append", partition_cols, io_config)

    # Write some new data
    write(new_data, path, format, write_mode, partition_cols, io_config)

    # Read back the data
    read_path = path + "/**" if partition_cols is not None else path
    read_back = read(read_path, format, io_config).sort(["a", "b"]).to_pydict()

    return read_back


@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("num_partitions", [1, 2])
@pytest.mark.parametrize("partition_cols", [None, ["a"]])
def test_write_modes_local(tmp_path, write_mode, format, num_partitions, partition_cols):
    path = str(tmp_path)
    existing_data = {"a": ["a", "a", "b", "b"], "b": [1, 2, 3, 4]}
    new_data = {
        "a": ["a", "a", "b", "b"],
        "b": [5, 6, 7, 8],
    }

    read_back = arrange_write_mode_test(
        daft.from_pydict(existing_data).into_partitions(num_partitions),
        daft.from_pydict(new_data).into_partitions(num_partitions),
        path,
        format,
        write_mode,
        partition_cols,
        None,
    )

    # Check the data
    if write_mode == "append":
        assert read_back["a"] == ["a"] * 4 + ["b"] * 4
        assert read_back["b"] == [1, 2, 5, 6, 3, 4, 7, 8]
    elif write_mode == "overwrite":
        assert read_back["a"] == ["a", "a", "b", "b"]
        assert read_back["b"] == [5, 6, 7, 8]
    else:
        raise ValueError(f"Unsupported write_mode: {write_mode}")


@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
def test_write_modes_local_empty_data(tmp_path, write_mode, format):
    path = str(tmp_path)
    existing_data = {"a": ["a", "a", "b", "b"], "b": ["c", "d", "e", "f"]}
    new_data = {
        "a": ["a", "a", "b", "b"],
        "b": ["g", "h", "i", "j"],
    }

    read_back = arrange_write_mode_test(
        daft.from_pydict(existing_data),
        daft.from_pydict(new_data).where(daft.lit(False)),  # Empty data
        path,
        format,
        write_mode,
        None,
        None,
    )

    # Check the data
    if write_mode == "append":
        # The data should be the same as the existing data
        assert read_back["a"] == ["a", "a", "b", "b"]
        assert read_back["b"] == ["c", "d", "e", "f"]
    elif write_mode == "overwrite":
        # The data should be empty because we are overwriting the existing data
        assert read_back["a"] == []
        assert read_back["b"] == []
    else:
        raise ValueError(f"Unsupported write_mode: {write_mode}")


@pytest.fixture(scope="function")
def bucket(minio_io_config):
    BUCKET = "write-modes-bucket"

    fs = s3fs.S3FileSystem(
        key=minio_io_config.s3.key_id,
        password=minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": minio_io_config.s3.endpoint_url},
    )
    if not fs.exists(BUCKET):
        fs.mkdir(BUCKET)
    yield BUCKET


@pytest.mark.integration()
@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("num_partitions", [1, 2])
@pytest.mark.parametrize("partition_cols", [None, ["a"]])
def test_write_modes_s3_minio(
    minio_io_config,
    bucket,
    write_mode,
    format,
    num_partitions,
    partition_cols,
):
    path = f"s3://{bucket}/{str(uuid.uuid4())}"
    existing_data = {"a": ["a", "a", "b", "b"], "b": [1, 2, 3, 4]}
    new_data = {
        "a": ["a", "a", "b", "b"],
        "b": [5, 6, 7, 8],
    }

    read_back = arrange_write_mode_test(
        daft.from_pydict(existing_data).into_partitions(num_partitions),
        daft.from_pydict(new_data).into_partitions(num_partitions),
        path,
        format,
        write_mode,
        partition_cols,
        minio_io_config,
    )

    # Check the data
    if write_mode == "append":
        assert read_back["a"] == ["a"] * 4 + ["b"] * 4
        assert read_back["b"] == [1, 2, 5, 6, 3, 4, 7, 8]
    elif write_mode == "overwrite":
        assert read_back["a"] == ["a", "a", "b", "b"]
        assert read_back["b"] == [5, 6, 7, 8]
    else:
        raise ValueError(f"Unsupported write_mode: {write_mode}")


@pytest.mark.integration()
@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
def test_write_modes_s3_minio_empty_data(
    minio_io_config,
    bucket,
    write_mode,
    format,
):
    path = f"s3://{bucket}/{str(uuid.uuid4())}"
    existing_data = {"a": ["a", "a", "b", "b"], "b": ["c", "d", "e", "f"]}
    new_data = {
        "a": ["a", "a", "b", "b"],
        "b": ["g", "h", "i", "j"],
    }

    read_back = arrange_write_mode_test(
        daft.from_pydict(existing_data),
        daft.from_pydict(new_data).where(daft.lit(False)),  # Empty data
        path,
        format,
        write_mode,
        None,
        minio_io_config,
    )

    # Check the data
    if write_mode == "append":
        # The data should be the same as the existing data
        assert read_back["a"] == ["a", "a", "b", "b"]
        assert read_back["b"] == ["c", "d", "e", "f"]
    elif write_mode == "overwrite":
        # The data should be empty because we are overwriting the existing data
        assert read_back["a"] == []
        assert read_back["b"] == []
    else:
        raise ValueError(f"Unsupported write_mode: {write_mode}")
