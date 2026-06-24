from __future__ import annotations

import io
import re

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray", reason="Merge scan tasks are only supported on the Ray runner"
)


@pytest.fixture(scope="function")
def parquet_files(tmpdir):
    """Writes 1 Parquet file with 10 rowgroups, each of 100 bytes in size."""
    tbl = pa.table({"data": ["aaa"] * 100})
    path = tmpdir / "file.pq"
    papq.write_table(tbl, str(path), row_group_size=10, use_dictionary=False)

    return tmpdir


def test_split_parquet_read(parquet_files):
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=10,
    ):
        df = daft.read_parquet(str(parquet_files))
        assert df.num_partitions() == 10, "Should have 10 partitions since we will split the file"
        assert df.to_pydict() == {"data": ["aaa"] * 100}


def test_split_parquet_estimate_tracks_uncompressed_size(tmpdir):
    """Each split ScanTask should carry its own per-column sizes.

    Regression test for two related gaps:
      * split tasks for the first file reused the *whole-file* column sizes (only
        `length` was updated), so each split estimated the entire file; and
      * split tasks for the remaining files had no metadata and fell back to
        `parquet_inflation_factor`.

    With per-split column sizes, the total estimated scan bytes should track the
    files' real uncompressed size rather than being multiplied by the number of
    splits or blown up by the inflation factor.
    """
    # Two files, each with 20 row groups of low-cardinality data.
    paths = []
    uncompressed = 0
    for f in range(2):
        tbl = pa.table({"data": [f"val_{i % 4}" for i in range(1000)]})
        path = tmpdir / f"file_{f}.pq"
        papq.write_table(tbl, str(path), row_group_size=50, use_dictionary=False)
        paths.append(str(path))
        md = papq.ParquetFile(str(path)).metadata
        uncompressed += sum(md.row_group(i).total_byte_size for i in range(md.num_row_groups))

    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=1,
        # Absurd inflation factor: if any split fell back to it (i.e. had no
        # per-column metadata), the estimate would explode well past the bound.
        parquet_inflation_factor=1000.0,
    ):
        df = daft.read_parquet(str(tmpdir))
        assert df.num_partitions() > 2, "files should split into many tasks"
        string_io = io.StringIO()
        df.explain(show_all=True, file=string_io)
        explain_output = string_io.getvalue()

    match = re.search(r"Estimated Scan Bytes = (\d+)", explain_output)
    assert match is not None, f"could not find scan bytes in explain output:\n{explain_output}"
    estimated = int(match.group(1))

    # Should track the real uncompressed size, not N x it (whole-file reuse) nor
    # compressed x 1000 (inflation fallback).
    assert 0.3 * uncompressed <= estimated <= 3 * uncompressed, (
        f"estimated scan bytes {estimated} should be close to uncompressed size {uncompressed}"
    )


@pytest.mark.skip(reason="Not implemented yet")
def test_split_parquet_read_some_splits(tmpdir):
    with daft.execution_config_ctx(enable_scan_task_split_and_merge=True):
        # Write a mix of 20 large and 20 small files
        # Small ones should not be split, large ones should be split into 10 rowgroups each
        # This gives us a total of 200 + 20 scantasks

        # Write 20 large files into tmpdir
        large_file_paths = []
        for i in range(20):
            tbl = pa.table({"data": [str(f"large{i}") for i in range(100)]})
            path = tmpdir / f"file.{i}.large.pq"
            papq.write_table(tbl, str(path), row_group_size=10, use_dictionary=False)
            large_file_paths.append(str(path))

        # Write 20 small files into tmpdir
        small_file_paths = []
        for i in range(20):
            tbl = pa.table({"data": ["small"]})
            path = tmpdir / f"file.{i}.small.pq"
            papq.write_table(tbl, str(path), row_group_size=1, use_dictionary=False)
            small_file_paths.append(str(path))

        # Test [large_paths, ..., small_paths, ...]
        with daft.execution_config_ctx(
            enable_scan_task_split_and_merge=True,
            scan_tasks_min_size_bytes=20,
            scan_tasks_max_size_bytes=100,
        ):
            df = daft.read_parquet(large_file_paths + small_file_paths)
            assert df.num_partitions() == 220, (
                "Should have 220 partitions since we will split all large files (20 * 10 rowgroups) but keep small files unsplit"
            )
            assert df.to_pydict() == {"data": [str(f"large{i}") for i in range(100)] * 20 + ["small"] * 20}

        # Test interleaved [large_path, small_path, large_path, small_path, ...]
        with daft.execution_config_ctx(
            enable_scan_task_split_and_merge=True,
            scan_tasks_min_size_bytes=20,
            scan_tasks_max_size_bytes=100,
        ):
            interleaved_paths = [path for pair in zip(large_file_paths, small_file_paths) for path in pair]
            df = daft.read_parquet(interleaved_paths)
            assert df.num_partitions() == 220, (
                "Should have 220 partitions since we will split all large files (20 * 10 rowgroups) but keep small files unsplit"
            )
            assert df.to_pydict() == {"data": ([str(f"large{i}") for i in range(100)] + ["small"]) * 20}

        # Test [small_paths, ..., large_paths]
        with daft.execution_config_ctx(
            enable_scan_task_split_and_merge=True,
            scan_tasks_min_size_bytes=20,
            scan_tasks_max_size_bytes=100,
        ):
            df = daft.read_parquet(small_file_paths + large_file_paths)
            assert df.num_partitions() == 220, (
                "Should have 220 partitions since we will split all large files (20 * 10 rowgroups) but keep small files unsplit"
            )
            assert df.to_pydict() == {"data": ["small"] * 20 + [str(f"large{i}") for i in range(100)] * 20}


def test_split_jsonl_byte_ranges(tmpdir):
    """Verify that byte-range sub-tasks read only their assigned range, not the entire file."""
    path = tmpdir / "data.jsonl"
    num_rows = 10_000
    payload = "x" * 150
    with open(str(path), "w", encoding="utf-8", newline="") as f:
        for i in range(num_rows):
            f.write(f'{{"id":{i},"payload":"{payload}"}}\n')

    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_max_size_bytes=512 * 1024,
        scan_tasks_min_size_bytes=0,
    ):
        df = daft.read_json(str(path))
        result = df.to_pydict()
        ids = result["id"]
        assert len(ids) == num_rows, f"Expected {num_rows} rows, got {len(ids)} (data may be duplicated)"
        assert len(set(ids)) == num_rows, "Duplicate IDs found — byte ranges likely ignored"
