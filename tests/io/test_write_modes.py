import uuid
from typing import List, Optional

import pytest

import daft
from daft import context
from tests.integration.io.conftest import minio_create_bucket

pytestmark = pytest.mark.skipif(
    context.get_context().daft_execution_config.enable_native_executor is True,
    reason="Native executor doesn't support writes yet",
)


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
    read_back = read(read_path, format, io_config).sort("a").to_pydict()

    return read_back


@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("num_partitions", [1, 50, 100])
@pytest.mark.parametrize("partition_cols", [None, ["a"]])
def test_write_modes_local(tmp_path, write_mode, format, num_partitions, partition_cols):
    path = str(tmp_path)
    existing_data = {"a": [i for i in range(100)]}
    new_data = {
        "a": [i for i in range(100, 200)],
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
        assert read_back["a"] == existing_data["a"] + new_data["a"]
    elif write_mode == "overwrite":
        assert read_back["a"] == new_data["a"]
    else:
        raise ValueError(f"Unsupported write_mode: {write_mode}")


@pytest.mark.integration()
@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("num_partitions", [1, 2])
@pytest.mark.parametrize("partition_cols", [None, ["a"]])
def test_write_modes_s3_minio(
    minio_io_config,
    write_mode,
    format,
    num_partitions,
    partition_cols,
):
    bucket_name = "my-bucket2"
    path = f"s3://{bucket_name}/write_modes_s3_minio-{uuid.uuid4()}"
    with minio_create_bucket(minio_io_config=minio_io_config, bucket_name=bucket_name):
        existing_data = {"a": [i for i in range(10)]}
        new_data = {
            "a": [i for i in range(10, 20)],
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
            assert read_back["a"] == existing_data["a"] + new_data["a"]
        elif write_mode == "overwrite":
            assert read_back["a"] == new_data["a"]
        else:
            raise ValueError(f"Unsupported write_mode: {write_mode}")
