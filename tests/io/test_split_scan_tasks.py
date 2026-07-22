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


def test_split_parquet_count_rows(parquet_files):
    """Regression: count_rows() must not N-times overcount a split file.

    When a file is split into N subtasks, each subtask's count-pushdown shortcut
    used to ignore its row-group constraint (ChunkSpec) and return the WHOLE-file
    row count, so the sum over subtasks was N x the true count — a silent
    data-correctness error on the officially recommended split configuration.
    """
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=10,
    ):
        df = daft.read_parquet(str(parquet_files))
        assert df.num_partitions() == 10, "file should split into 10 subtasks"
        # Fixture is 100 rows; before the fix this returned 100 * 10 = 1000.
        assert df.count_rows() == 100
        # Agrees with an explicit count aggregation (which never takes the shortcut).
        assert df.agg(daft.col("data").count()).to_pydict()["data"][0] == 100


def test_split_parquet_estimate_uses_materialized_size(tmpdir):
    """The scan size estimate must reflect the materialized buffer size, not encoded size.

    For dictionary/RLE-encoded int64 lists, the encoded size is many times smaller than
    the decoded Arrow buffer. The estimate is derived from value counts and column types
    (num_values * 8 for int64), so it should track the materialized size. This also guards
    against the split path either reusing whole-file sizes (N x over-estimate) or dropping
    metadata and falling back to the inflation factor.
    """
    n_rows = 5000
    seq = list(range(128))  # repetitive -> highly dictionary-compressible
    tbl = pa.table(
        {"position_ids": [seq for _ in range(n_rows)]},
        schema=pa.schema([("position_ids", pa.list_(pa.int64()))]),
    )
    path = tmpdir / "file.pq"
    papq.write_table(tbl, str(path), use_dictionary=True, row_group_size=1000)

    md = papq.ParquetFile(str(path)).metadata
    uncompressed = 0
    num_values = 0
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        for c in range(rg.num_columns):
            uncompressed += rg.column(c).total_uncompressed_size
            num_values += rg.column(c).num_values
    materialized = num_values * 8  # int64 leaf buffer

    # Tiny byte thresholds: splitting is gated on the (small, dictionary-compressed)
    # on-disk size, so use 1 to force one split per row group.
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=1,
    ):
        df = daft.read_parquet(str(path))
        assert df.num_partitions() > 1, "file should split into multiple tasks"
        string_io = io.StringIO()
        df.explain(show_all=True, file=string_io)
        explain_output = string_io.getvalue()

    match = re.search(r"Estimated Scan Bytes = (\d+)", explain_output)
    assert match is not None, f"could not find scan bytes in explain output:\n{explain_output}"
    estimated = int(match.group(1))

    # Tracks the materialized buffer size...
    assert 0.8 * materialized <= estimated <= 1.2 * materialized, (
        f"estimated {estimated} should be ~materialized {materialized}"
    )
    # ...and is far larger than the encoded size we used to report.
    assert estimated > 3 * uncompressed, f"estimated {estimated} should be >> uncompressed {uncompressed}"


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
