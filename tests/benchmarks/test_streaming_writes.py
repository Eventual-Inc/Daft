from __future__ import annotations

import pytest

import daft
from tests.benchmarks.conftest import IS_CI

ENGINES = ["native", "python"]


@pytest.mark.skipif(IS_CI, reason="Write benchmarks are not run in CI")
@pytest.mark.benchmark(group="write")
@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize(
    "file_type, target_file_size, target_row_group_size",
    [
        ("parquet", None, None),
        (
            "parquet",
            5 * 1024 * 1024,
            1024 * 1024,
        ),  # 5MB target file size, 1MB target row group size
        ("csv", None, None),
    ],
)
@pytest.mark.parametrize("partition_cols", [None, ["L_SHIPMODE"]])
@pytest.mark.parametrize("get_df", ["in-memory"], indirect=True)
def test_streaming_write(
    tmp_path,
    get_df,
    benchmark_with_memray,
    engine,
    file_type,
    target_file_size,
    target_row_group_size,
    partition_cols,
):
    get_df, num_parts = get_df
    daft_df = get_df("lineitem")

    def f():
        if engine == "native":
            daft.context.set_runner_native()
        else:
            raise ValueError(f"{engine} unsupported")

        ctx = daft.context.execution_config_ctx(
            parquet_target_filesize=target_file_size,
            parquet_target_row_group_size=target_row_group_size,
            csv_target_filesize=target_file_size,
        )

        with ctx:
            if file_type == "parquet":
                return daft_df.write_parquet(tmp_path, partition_cols=partition_cols)
            elif file_type == "csv":
                return daft_df.write_csv(tmp_path, partition_cols=partition_cols)
            else:
                raise ValueError(f"{file_type} unsupported")

    benchmark_group = f"parts-{num_parts}-partition-cols-{partition_cols}-file-type-{file_type}-target-file-size-{target_file_size}-target-row-group-size-{target_row_group_size}"
    result_files = benchmark_with_memray(f, benchmark_group).to_pydict()["path"]
    read_back = daft.read_parquet(result_files) if file_type == "parquet" else daft.read_csv(result_files)
    assert read_back.count_rows() == daft_df.count_rows()
