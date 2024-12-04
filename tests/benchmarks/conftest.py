from __future__ import annotations

import os
import tempfile
from collections import defaultdict

import memray
import pytest
from fsspec.implementations.local import LocalFileSystem

import daft
from benchmarking.tpch import data_generation
from tests.assets import TPCH_DBGEN_DIR

IS_CI = True if os.getenv("CI") else False

SCALE_FACTOR = 0.2
NUM_PARTS = [1] if IS_CI else [1, 2]
SOURCE_TYPES = ["in-memory"] if IS_CI else ["parquet", "in-memory"]

memray_stats = defaultdict(dict)


def pytest_terminal_summary(terminalreporter):
    if memray_stats:
        for group, group_stats in sorted(memray_stats.items()):
            terminalreporter.write_sep("-", f"Memray Stats for Group: {group}")
            for nodeid, stats in group_stats.items():
                terminalreporter.write_line(
                    f"{nodeid} \t Peak Memory: {stats['peak_memory']} MB \t Total Allocations: {stats['total_allocations']}"
                )
                terminalreporter.ensure_newline()


@pytest.fixture
def benchmark_with_memray(request, benchmark):
    def track_mem(func, group):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file_path = os.path.join(tmpdir, "memray_output.bin")
            with memray.Tracker(output_file_path):
                res = func()

            reader = memray.FileReader(output_file_path)
            stats = {
                "peak_memory": reader.metadata.peak_memory / 1024 / 1024,
                "total_allocations": reader.metadata.total_allocations,
            }
            memray_stats[group][request.node.nodeid] = stats

        return res

    def benchmark_wrapper(func, group):
        benchmark.group = group
        # If running in CI, just run the benchmark
        if os.getenv("CI"):
            return benchmark(func)
        else:
            benchmark(func)
            return track_mem(func, group)

    return benchmark_wrapper


@pytest.fixture(scope="session", params=NUM_PARTS)
def gen_tpch(request):
    # Parametrize the number of parts for each file so that we run tests on single-partition files and multi-partition files
    num_parts = request.param

    csv_files_location = data_generation.gen_csv_files(TPCH_DBGEN_DIR, num_parts, SCALE_FACTOR)
    parquet_files_location = data_generation.gen_parquet(csv_files_location)

    in_memory_tables = {}
    for tbl_name in data_generation.SCHEMA.keys():
        arrow_table = daft.read_parquet(f"{parquet_files_location}/{tbl_name}/*").to_arrow()
        in_memory_tables[tbl_name] = daft.from_arrow(arrow_table)

    sqlite_path = data_generation.gen_sqlite_db(
        csv_filepath=csv_files_location,
        num_parts=num_parts,
    )

    return (
        csv_files_location,
        parquet_files_location,
        in_memory_tables,
        num_parts,
    ), sqlite_path


@pytest.fixture(scope="module", params=SOURCE_TYPES)
def get_df(gen_tpch, request):
    (csv_files_location, parquet_files_location, in_memory_tables, num_parts), _ = gen_tpch
    source_type = request.param

    def _get_df(tbl_name: str):
        if source_type == "csv":
            local_fs = LocalFileSystem()
            nonchunked_filepath = f"{csv_files_location}/{tbl_name}.tbl"
            chunked_filepath = nonchunked_filepath + ".*"
            try:
                local_fs.expand_path(chunked_filepath)
                fp = chunked_filepath
            except FileNotFoundError:
                fp = nonchunked_filepath

            df = daft.read_csv(
                fp,
                has_headers=False,
                delimiter="|",
            )
            df = df.select(
                *[
                    daft.col(autoname).alias(colname)
                    for autoname, colname in zip(df.column_names, data_generation.SCHEMA[tbl_name])
                ]
            )
        elif source_type == "parquet":
            fp = f"{parquet_files_location}/{tbl_name}/*"
            df = daft.read_parquet(fp)
        elif source_type == "in-memory":
            df = in_memory_tables[tbl_name]

        return df

    return _get_df, num_parts
