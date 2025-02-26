from __future__ import annotations

import os
import shutil
import tempfile

import pytest

import daft
from daft import DataFrame
from tests.conftest import get_tests_daft_runner_name


@pytest.fixture(scope="module", params=[(1, 64), (8, 8), (64, 1)], ids=["1x64mib", "8x8mib", "64x1mib"])
def gen_simple_csvs(request) -> str:
    print(f"request: {request}")
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

        print(f"tmpdirname: {tmpdirname}, num_files: {num_files}, mibs_per_file: {mibs_per_file}")
        yield tmpdirname, num_files * mibs_per_file * 1024 * 128
        print("yielded")
    print("done")


@pytest.mark.skipif(get_tests_daft_runner_name() not in {"native", "py"}, reason="requires local runner")
@pytest.mark.benchmark(group="file_read")
def test_csv_read(gen_simple_csvs, benchmark):
    csv_dir, num_rows = gen_simple_csvs
    print(f"csv_dir: {csv_dir}, num_rows: {num_rows}")

    def bench() -> DataFrame:
        df = daft.read_csv(csv_dir)
        print("read ok")
        return df.collect()

    df = benchmark(bench)
    print(f"len(df): {len(df)}")
    assert len(df) == num_rows
    print("assert ok")


@pytest.mark.benchmark(group="file_read")
@pytest.mark.parametrize("prune", [True, False])
def test_s3_parquet_read_1x64mb(benchmark, prune):
    print(f"test_s3_parquet_read_1x64mb, prune: {prune}")
    parquet_glob = "s3://daft-public-data/test_fixtures/parquet/95c7fba0-265d-440b-88cb-2897047fc5f9-0.parquet"
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
    print(f"test_s3_parquet_read_32x2mb, prune: {prune}")
    parquet_glob = "s3://daft-public-data/test_fixtures/parquet_small/*"
    expected_rows = 2000000

    def bench() -> DataFrame:
        df = daft.read_parquet(parquet_glob)
        if prune:
            df = df.select(df["P_SIZE"])  # rightmost int64 column
        return df.collect()

    df = benchmark(bench)
    assert len(df.to_pandas()) == expected_rows
