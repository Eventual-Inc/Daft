from __future__ import annotations

from collections import Counter

import pytest

import daft
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray", reason="Merge scan tasks are only supported on the Ray runner"
)


@pytest.fixture(scope="function")
def csv_files(tmpdir):
    """Writes 3 CSV files, each of 10 bytes in size, to tmpdir and yield tmpdir."""
    for i in range(3):
        path = tmpdir / f"file.{i}.csv"
        path.write_text("a,b,c\n1,2,", "utf8")  # 10 bytes

    return tmpdir


def test_merge_scan_task_exceed_max(csv_files):
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=0,
        scan_tasks_max_size_bytes=0,
    ):
        df = daft.read_csv(str(csv_files))
        assert df.num_partitions() == 3, (
            "Should have 3 partitions since all merges are more than the maximum (>0 bytes)"
        )


def test_merge_scan_task_below_max(csv_files):
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=11,
        scan_tasks_max_size_bytes=12,
    ):
        df = daft.read_csv(str(csv_files))
        assert df.num_partitions() == 2, (
            "Should have 2 partitions [(CSV1, CSV2), (CSV3)] since the second merge is too large (>22 bytes)"
        )


def test_merge_scan_task_above_min(csv_files):
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=9,
        scan_tasks_max_size_bytes=20,
    ):
        df = daft.read_csv(str(csv_files))
        assert df.num_partitions() == 2, (
            "Should have 2 partitions [(CSV1, CSV2), (CSV3)] since the first merge is above the minimum (>19 bytes)"
        )


def test_merge_scan_task_below_min(csv_files):
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=17,
        scan_tasks_max_size_bytes=20,
    ):
        df = daft.read_csv(str(csv_files))
        assert df.num_partitions() == 1, (
            "Should have 1 partition [(CSV1, CSV2, CSV3)] since both merges are below the minimum and maximum"
        )


def test_merge_scan_task_limit_override(csv_files):
    # A LIMIT operation should override merging of scan tasks, making it only merge up-to the estimated size of the limit
    #
    # With a very small CSV inflation factor, the merger will think that these CSVs provide very few rows of data and will be more aggressive
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=17,
        scan_tasks_max_size_bytes=20,
        csv_inflation_factor=0.1,
    ):
        df = daft.read_csv(str(csv_files)).limit(1)
        assert df.num_partitions() == 1, (
            "Should have 1 partitions [(CSV1, CSV2, CSV3)] since we have a limit 1 but are underestimating the size of data of the CSVs"
        )

    # With a very large CSV inflation factor, the merger will think that these CSVs provide more rows of data and will be more conservative
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=17,
        scan_tasks_max_size_bytes=20,
        csv_inflation_factor=2.0,
    ):
        df = daft.read_csv(str(csv_files)).limit(1)
        assert df.num_partitions() == 3, "Should have 3 partitions [(CSV1, CSV2, CSV3)] since we have a limit 1"


def test_merge_scan_task_up_to_max_sources(csv_files):
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=30,
        scan_tasks_max_size_bytes=30,
        max_sources_per_scan_task=2,
    ):
        df = daft.read_csv(str(csv_files))
        assert df.num_partitions() == 2, (
            "Should have 2 partitions [(CSV1, CSV2), (CSV3)] since the third CSV is too large to merge with the first two, and max_sources_per_scan_task is set to 2"
        )


@pytest.mark.parametrize(
    "min_size_bytes, max_size_bytes",
    [
        (0, 0),
        (0, 10000),
        (10000, 0),
        (10000, 10000),
    ],
)
def test_merge_scan_tasks_does_not_merge_warc(min_size_bytes, max_size_bytes):
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=min_size_bytes,
        scan_tasks_max_size_bytes=max_size_bytes,
    ):
        path = ["tests/assets/example.warc"] * 3
        df = daft.read_warc(path)
        assert df.num_partitions() == 3, "Should have 3 partitions since WARC files are not merged"


def test_merge_scan_task_with_file_path_column(tmp_path):
    """Merge succeeds when file_path_column is provided."""
    for i in range(3):
        (tmp_path / f"file_{i}.csv").write_text("x,y\n1,2\n3,4\n")

    expected_paths = {str(tmp_path / f"file_{i}.csv") for i in range(3)}

    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=100,
        scan_tasks_max_size_bytes=10000,
    ):
        df = daft.read_csv(str(tmp_path), file_path_column="source")
        assert df.num_partitions() == 1, "All 3 tiny CSVs should merge into a single partition"
        result = df.to_pydict()
        assert "source" in result, "file_path_column 'source' must be present in the result"
        assert len(result["source"]) == 6, "3 files x 2 rows each = 6 rows total"
        assert set(result["source"]) == expected_paths, "file_path_column values should be the actual file paths"


def test_merge_scan_task_file_path_column_values_correct(tmp_path):
    """After merging, each row's file_path_column must point to its originating file."""
    files = []
    for i in range(3):
        p = tmp_path / f"data_{i}.csv"
        p.write_text(f"id,val\n{i},a\n{i},b\n")
        files.append(str(p))

    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=100,
        scan_tasks_max_size_bytes=10000,
    ):
        df = daft.read_csv(files, file_path_column="path")
        assert df.num_partitions() == 1, "All files should merge into one partition"
        result = df.sort("id").to_pydict()

        # Each file writes rows where id == file index, so we can map each row
        # back to its originating file deterministically.
        for row_idx in range(len(result["id"])):
            row_id = result["id"][row_idx]
            expected_path = files[row_id]
            actual_path = result["path"][row_idx]
            assert actual_path == expected_path, (
                f"Row {row_idx}: id={row_id} should come from {expected_path}, got {actual_path}"
            )

        path_counts = Counter(result["path"])
        for f in files:
            assert path_counts[f] == 2, f"File {f} should contribute exactly 2 rows, got {path_counts[f]}"


def test_merge_scan_task_without_file_path_column_no_regression(tmp_path):
    """Merging without file_path_column still works (no regression)."""
    for i in range(3):
        (tmp_path / f"file_{i}.csv").write_text("a,b\n10,20\n30,40\n")

    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=100,
        scan_tasks_max_size_bytes=10000,
    ):
        df = daft.read_csv(str(tmp_path))
        assert df.num_partitions() == 1, "All files should merge into one partition"
        result = df.to_pydict()
        assert set(result.keys()) == {"a", "b"}, "Only original columns should be present, no file_path_column"
        assert len(result["a"]) == 6, "3 files x 2 rows each = 6 rows total"
        assert sorted(result["a"]) == [10, 10, 10, 30, 30, 30]
        assert sorted(result["b"]) == [20, 20, 20, 40, 40, 40]


def test_merge_scan_task_hive_partitioned(tmp_path):
    """Merging scan tasks from different hive partition values preserves correct partition column values."""
    for year in [2023, 2024]:
        d = tmp_path / f"year={year}"
        d.mkdir()
        for i in range(2):
            (d / f"data_{i}.csv").write_text(f"id,val\n{year * 10 + i},a\n")

    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=100,
        scan_tasks_max_size_bytes=10000,
    ):
        df = daft.read_csv(str(tmp_path / "**"), hive_partitioning=True)
        assert df.num_partitions() < 4, "Some scan tasks should be merged across hive partition values"
        result = df.sort("id").to_pydict()
        assert len(result["id"]) == 4, "2 partitions x 2 files x 1 row = 4 rows total"
        assert "year" in result, "Hive partition column 'year' must be present"
        for row_idx in range(len(result["id"])):
            row_id = result["id"][row_idx]
            expected_year = row_id // 10
            assert result["year"][row_idx] == expected_year, (
                f"Row {row_idx}: id={row_id} should have year={expected_year}, got {result['year'][row_idx]}"
            )
