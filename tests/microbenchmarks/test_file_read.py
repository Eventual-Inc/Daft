from __future__ import annotations

import os
import shutil
import tempfile

import pytest

import daft
from daft import DataFrame
from daft.context import _RayRunnerConfig, get_context


def is_using_remote_runner() -> bool:
    runner_config = get_context().runner_config
    return isinstance(runner_config, _RayRunnerConfig) and runner_config.address is not None


@pytest.fixture(scope="module", params=[(1, 64), (8, 8), (64, 1)], ids=["1x64mib", "8x8mib", "64x1mib"])
def gen_simple_csvs(request) -> str:
    """Creates some CSVs in a directory. Returns the name of the directory."""

    num_files, mibs_per_file = request.param

    _8bytes = b"aaa,bbb\n"
    _1kib = _8bytes * 128
    _1mib = _1kib * 1024

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Make one CSV file of the correct size.
        with open(os.path.join(tmpdirname, "file.csv"), "wb") as f:
            f.write(b"A,B\n")
            for i in range(mibs_per_file):
                f.write(_1mib)

        # Copy it to get the remaining number of desired files.
        for i in range(1, num_files):
            shutil.copyfile(
                src=os.path.join(tmpdirname, "file.csv"),
                dst=os.path.join(tmpdirname, f"file{i}.csv"),
            )

        yield tmpdirname, num_files * mibs_per_file * 1024 * 128


@pytest.mark.skipif(is_using_remote_runner(), reason="requires local runner")
@pytest.mark.benchmark(group="file_read")
def test_csv_read(gen_simple_csvs, benchmark):
    csv_dir, num_rows = gen_simple_csvs

    def bench() -> DataFrame:
        df = daft.read_csv(csv_dir)
        return df.collect()

    df = benchmark(bench)

    assert len(df) == num_rows


@pytest.mark.benchmark(group="file_read")
@pytest.mark.parametrize("prune", [True, False])
def test_s3_parquet_read_1x64mb(benchmark, prune):
    parquet_glob = "s3://daft-public-data/test_fixtures/parquet/*"
    expected_rows = 1500000

    def bench() -> DataFrame:
        df = daft.read_parquet(parquet_glob)
        if prune:
            df = df.select(df["O_SHIPPRIORITY"])  # rightmost int64 column
        return df.collect()

    df = benchmark(bench)
    assert len(df.to_pandas()) == expected_rows


@pytest.mark.benchmark(group="file_read")
@pytest.mark.parametrize("prune", [True, False])
def test_s3_parquet_read_32x2mb(benchmark, prune):
    parquet_glob = "s3://daft-public-data/test_fixtures/parquet_small/*"
    expected_rows = 2000000

    def bench() -> DataFrame:
        df = daft.read_parquet(parquet_glob)
        if prune:
            df = df.select(df["P_SIZE"])  # rightmost int64 column
        return df.collect()

    df = benchmark(bench)
    assert len(df.to_pandas()) == expected_rows
