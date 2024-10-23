from typing import Optional

import pytest

import daft


def write(
    df: daft.DataFrame,
    path: str,
    format: str,
    write_mode: str,
    partition_col: Optional[str] = None,
):
    if format == "parquet":
        return df.write_parquet(path, write_mode=write_mode, partition_cols=[partition_col])
    elif format == "csv":
        return df.write_csv(path, write_mode=write_mode, partition_cols=[partition_col])
    else:
        raise ValueError(f"Unsupported format: {format}")


def read(path: str, format: str):
    if format == "parquet":
        return daft.read_parquet(path)
    elif format == "csv":
        return daft.read_csv(path)
    else:
        raise ValueError(f"Unsupported format: {format}")


@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("num_partitions", [1, 50, 100])
@pytest.mark.parametrize("partition_cols", [None, ["a"]])
def test_write_modes_local(tmp_path, write_mode, format, num_partitions, partition_cols):
    path = str(tmp_path)
    existing_data = {"a": [i for i in range(100)]}
    # Write some existing_data
    write(
        daft.from_pydict(existing_data).into_partitions(num_partitions),
        path,
        format,
        "append",
        partition_cols,
    )

    # Write some new data
    new_data = {
        "a": [i for i in range(100, 200)],
    }
    write(
        daft.from_pydict(new_data).into_partitions(num_partitions),
        path,
        format,
        write_mode,
        partition_cols,
    )

    # Read back the data
    read_path = path + "/**" if partition_cols is not None else path
    read_back = read(read_path, format).sort("a").to_pydict()

    # Check the data
    if write_mode == "append":
        assert read_back["a"] == existing_data["a"] + new_data["a"]
    elif write_mode == "overwrite":
        assert read_back["a"] == new_data["a"]
    else:
        raise ValueError(f"Unsupported write_mode: {write_mode}")
