from __future__ import annotations

import gzip

import pytest

import daft
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="CSV split tests run on native runner")


def test_csv_split_local_equals_unsplit(tmp_path):
    """Single large CSV file should be split and produce the same data as unsplit."""
    csv_path = tmp_path / "test-split.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("id,value,name\n")
        for i in range(10000):
            f.write(f"{i},{i * 2},item_{i}\n")

    df_unsplit = daft.read_csv(str(csv_path))
    assert df_unsplit.count_rows() == 10000

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=128 * 1024,
    ):
        df_split = daft.read_csv(str(csv_path))
        split_count = df_split.count_rows()

    assert split_count == 10000


def test_csv_split_quoted_newlines(tmp_path):
    """CSV with quoted fields containing newlines should split correctly."""
    csv_path = tmp_path / "test-quoted.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("id,text\n")
        for i in range(5000):
            f.write(f'{i},"line1\nline2"\n')

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=64 * 1024,
    ):
        df = daft.read_csv(str(csv_path))
        assert df.count_rows() == 5000


def test_csv_compressed_not_split(tmp_path):
    """Compressed CSV files should not be split."""
    csv_path = tmp_path / "test.csv.gz"
    with gzip.open(csv_path, "wt", encoding="utf-8") as f:
        f.write("id,value\n")
        for i in range(10000):
            f.write(f"{i},{i * 2}\n")

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=128,
    ):
        df = daft.read_csv(str(csv_path))
        assert df.count_rows() == 10000


def test_csv_no_headers_split(tmp_path):
    """CSV without headers should split correctly."""
    csv_path = tmp_path / "test-no-header.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        for i in range(10000):
            f.write(f"{i},{i * 2},item_{i}\n")

    schema = {
        "id": daft.DataType.int64(),
        "value": daft.DataType.int64(),
        "name": daft.DataType.string(),
    }

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=128 * 1024,
    ):
        df = daft.read_csv(str(csv_path), has_headers=False, schema=schema)
        assert df.count_rows() == 10000


def test_csv_split_data_integrity(tmp_path):
    """Verify that split CSV data is identical to unsplit data."""
    csv_path = tmp_path / "test-integrity.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("id,value\n")
        for i in range(5000):
            f.write(f"{i},{i * 2}\n")

    df_unsplit = daft.read_csv(str(csv_path)).sort("id").collect()

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=64 * 1024,
    ):
        df_split = daft.read_csv(str(csv_path)).sort("id").collect()

    assert df_unsplit.to_pydict() == df_split.to_pydict()


def test_csv_multiple_files_split_and_merge(tmp_path):
    """Multiple CSV files: small ones merged, large ones split."""
    paths = []
    for k in range(5):
        p = tmp_path / f"part-{k}.csv"
        with p.open("w", encoding="utf-8") as f:
            f.write("id,value,name\n")
            for i in range(10000):
                f.write(f"{i},{i * 2},item_{i}\n")
        paths.append(str(p))

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=128 * 1024,
    ):
        df = daft.read_csv(paths)
        assert df.count_rows() == 50000
