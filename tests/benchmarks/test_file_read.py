from __future__ import annotations

import os
import shutil
import tempfile

import pytest

from daft import DataFrame


@pytest.fixture(scope="module", params=[(1, 64), (8, 8), (64, 1)])
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


@pytest.mark.benchmark(group="file_read")
def test_csv_read(gen_simple_csvs, benchmark):
    csv_dir, num_rows = gen_simple_csvs

    def bench() -> DataFrame:
        df = DataFrame.read_csv(csv_dir)
        return df.collect()

    df = benchmark(bench)

    assert len(df) == num_rows
