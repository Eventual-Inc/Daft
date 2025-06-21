from __future__ import annotations

import os
import pathlib
import shutil
import uuid

import pytest
import s3fs

import daft


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


@pytest.fixture(scope="function")
def tmp_relative_path():
    # Create a unique directory in the home directory
    base_relative_dir = pathlib.Path(f"~/test_dir_{uuid.uuid4()}")
    full_relative_dir = base_relative_dir.joinpath("relative", "path")

    # Return the ~ version for the test to use
    yield full_relative_dir

    # Remove the directory
    full_path = os.path.expanduser(base_relative_dir)
    shutil.rmtree(full_path)


def write(
    df: daft.DataFrame,
    path: str,
    format: str,
    write_mode: str,
    partition_cols: list[str] | None = None,
    io_config: daft.io.IOConfig | None = None,
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


def read(path: str, format: str, io_config: daft.io.IOConfig | None = None):
    if format == "parquet":
        return daft.read_parquet(path, io_config=io_config)
    elif format == "csv":
        return daft.read_csv(path, io_config=io_config)
    else:
        raise ValueError(f"Unsupported format: {format}")


def arrange_write_mode_test(
    existing_data,
    new_data,
    path,
    format,
    write_mode,
    partition_cols,
    io_config,
    sort_cols=None,
):
    # Write some existing_data
    write(existing_data, path, format, "append", partition_cols, io_config)
    # Write some new data
    write(new_data, path, format, write_mode, partition_cols, io_config)

    # Read back the data
    read_path = path + "/**" if partition_cols is not None else path
    read_back = read(read_path, format, io_config).collect()
    read_back = read_back.sort(sort_cols).to_pydict()

    return read_back


def _run_append_overwrite_test(
    path,
    write_mode,
    format,
    num_partitions,
    partition_cols,
    io_config,
):
    existing_data = daft.range(0, 15, partitions=num_partitions)
    new_data = daft.range(15, 30, partitions=num_partitions)

    read_back = arrange_write_mode_test(
        existing_data,
        new_data,
        path,
        format,
        write_mode,
        partition_cols,
        io_config,
        sort_cols=["id"],
    )

    # Check the data
    if write_mode == "append":
        assert read_back["id"] == list(range(30))
    elif write_mode == "overwrite":
        assert read_back["id"] == list(range(15, 30))
    else:
        raise ValueError(f"Unsupported write_mode: {write_mode}")


@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("num_partitions", [1, 10])
@pytest.mark.parametrize("partition_cols", [None, ["id"]])
def test_append_and_overwrite_local(tmp_path, write_mode, format, num_partitions, partition_cols):
    _run_append_overwrite_test(
        path=str(tmp_path),
        write_mode=write_mode,
        format=format,
        num_partitions=num_partitions,
        partition_cols=partition_cols,
        io_config=None,
    )


@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("num_partitions", [1, 10])
@pytest.mark.parametrize("partition_cols", [None, ["id"]])
def test_append_and_overwrite_local_relative_path(
    tmp_relative_path, write_mode, format, num_partitions, partition_cols
):
    _run_append_overwrite_test(
        path=str(tmp_relative_path),
        write_mode=write_mode,
        format=format,
        num_partitions=num_partitions,
        partition_cols=partition_cols,
        io_config=None,
    )


@pytest.mark.integration()
@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("num_partitions", [1, 10])
@pytest.mark.parametrize("partition_cols", [None, ["id"]])
def test_append_and_overwrite_s3_minio(
    minio_io_config,
    bucket,
    write_mode,
    format,
    num_partitions,
    partition_cols,
):
    _run_append_overwrite_test(
        path=f"s3://{bucket}/{uuid.uuid4()!s}",
        write_mode=write_mode,
        format=format,
        num_partitions=num_partitions,
        partition_cols=partition_cols,
        io_config=minio_io_config,
    )


def _run_write_modes_empty_test(
    path,
    write_mode,
    format,
    io_config,
):
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
        io_config,
        sort_cols=["a", "b"],
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


@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
def test_write_modes_local_empty_data(tmp_path, write_mode, format):
    _run_write_modes_empty_test(
        path=str(tmp_path),
        write_mode=write_mode,
        format=format,
        io_config=None,
    )


@pytest.mark.integration()
@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
def test_write_modes_s3_minio_empty_data(
    minio_io_config,
    bucket,
    write_mode,
    format,
):
    _run_write_modes_empty_test(
        path=f"s3://{bucket}/{uuid.uuid4()!s}",
        write_mode=write_mode,
        format=format,
        io_config=minio_io_config,
    )


OVERWRITE_PARTITION_TEST_CASES = [
    pytest.param(
        daft.range(0, 4, partitions=4).with_column("a", daft.lit("a")),
        daft.range(0, 4, partitions=4).with_column("a", daft.lit("b")),
        {
            "id": [0, 1, 2, 3],
            "a": ["b", "b", "b", "b"],
        },
        id="overwrite-all",
    ),
    pytest.param(
        daft.range(0, 4, partitions=4).with_column("a", daft.lit("a")),
        daft.range(0, 2, partitions=4).with_column("a", daft.lit("b")),
        {
            "id": [0, 1, 2, 3],
            "a": ["b", "b", "a", "a"],
        },
        id="overwrite-some",
    ),
    pytest.param(
        daft.range(0, 4, partitions=4).with_column("a", daft.lit("a")),
        daft.range(2, 6, partitions=4).with_column("a", daft.lit("b")),
        {
            "id": [0, 1, 2, 3, 4, 5],
            "a": ["a", "a", "b", "b", "b", "b"],
        },
        id="overwrite-and-append",
    ),
]


def _run_overwrite_partitions_test(
    path,
    format,
    existing_data,
    new_data,
    expected_read_back,
    io_config,
):
    read_back = arrange_write_mode_test(
        existing_data,
        new_data,
        path,
        format,
        "overwrite-partitions",
        ["id"],
        io_config,
        sort_cols=["id"],
    )

    # Check the data
    for col in expected_read_back:
        assert read_back[col] == expected_read_back[col]


@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("existing_data, new_data, expected_read_back", OVERWRITE_PARTITION_TEST_CASES)
def test_overwrite_partitions_local(tmp_path, format, existing_data, new_data, expected_read_back):
    _run_overwrite_partitions_test(
        path=str(tmp_path),
        format=format,
        existing_data=existing_data,
        new_data=new_data,
        expected_read_back=expected_read_back,
        io_config=None,
    )


@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("existing_data, new_data, expected_read_back", OVERWRITE_PARTITION_TEST_CASES)
def test_overwrite_partitions_local_relative_path(
    tmp_relative_path, format, existing_data, new_data, expected_read_back
):
    _run_overwrite_partitions_test(
        path=str(tmp_relative_path),
        format=format,
        existing_data=existing_data,
        new_data=new_data,
        expected_read_back=expected_read_back,
        io_config=None,
    )


@pytest.mark.integration()
@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("existing_data, new_data, expected_read_back", OVERWRITE_PARTITION_TEST_CASES)
def test_overwrite_partitions_s3_minio(
    minio_io_config,
    bucket,
    format,
    existing_data,
    new_data,
    expected_read_back,
):
    _run_overwrite_partitions_test(
        path=f"s3://{bucket}/{uuid.uuid4()!s}",
        format=format,
        existing_data=existing_data,
        new_data=new_data,
        expected_read_back=expected_read_back,
        io_config=minio_io_config,
    )
