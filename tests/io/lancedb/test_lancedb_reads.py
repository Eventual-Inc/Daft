from __future__ import annotations

import lance
import pyarrow as pa
import pytest

import daft
from daft import col

TABLE_NAME = "my_table"
data = {"vector": [[1.1, 1.2], [0.2, 1.8]], "lat": [45.5, 40.1], "long": [-122.7, -74.1], "big_int": [1, 2]}

PYARROW_LOWER_BOUND_SKIP = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (9, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LOWER_BOUND_SKIP, reason="lance not supported on old versions of pyarrow")


@pytest.fixture(scope="function")
def lance_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("lance")
    lance.write_dataset(pa.Table.from_pydict(data), tmp_dir)
    yield str(tmp_dir)


def test_lancedb_read(lance_dataset_path):
    df = daft.read_lance(lance_dataset_path)
    assert df.to_pydict() == data


def test_lancedb_read_column_selection(lance_dataset_path):
    df = daft.read_lance(lance_dataset_path)
    df = df.select("vector")
    assert df.to_pydict() == {"vector": data["vector"]}


def test_lancedb_read_filter(lance_dataset_path):
    df = daft.read_lance(lance_dataset_path)
    df = df.where((df["lat"] > 45) & (df["lat"] < 90))
    df = df.select("vector")
    assert df.to_pydict() == {"vector": data["vector"][:1]}


@pytest.fixture(scope="function")
def large_lance_dataset_path(tmp_path_factory):
    """Create a large Lance dataset with multiple fragments for testing limit operations."""
    tmp_dir = tmp_path_factory.mktemp("large_lance")

    # Create 10 fragments of 1000 rows each (10,000 total rows)
    for frag_idx in range(10):
        # Generate data for this fragment
        vectors = [[float(i * 0.1 + frag_idx * 1000), float(i * 0.2 + frag_idx * 1000)] for i in range(1000)]
        big_ints = [i + frag_idx * 1000 for i in range(1000)]

        fragment_data = {"vector": vectors, "big_int": big_ints}

        # Write fragment (first write creates dataset, subsequent writes append)
        mode = "append" if frag_idx > 0 else None
        lance.write_dataset(pa.Table.from_pydict(fragment_data), tmp_dir, mode=mode)

    yield str(tmp_dir)


@pytest.mark.parametrize(
    "limit_size,expected_scan_tasks",
    [
        # Small limits
        (1000, 1),
        (1001, 2),
        # Big limits
        (9000, 9),
        (9001, 10),
        (10000, 10),
    ],
)
def test_lancedb_read_limit_large_dataset(large_lance_dataset_path, limit_size, expected_scan_tasks):
    """Test limit operation on a large Lance dataset with multiple fragments."""
    import io

    df = daft.read_lance(large_lance_dataset_path)

    # Test with different limit sizes
    df = df.limit(limit_size)
    df = df.select("vector", "big_int")

    # Capture the explain output
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    explain_output = string_io.getvalue()

    # Assert that we have the expected number of scan tasks
    assert f"Num Scan Tasks = {expected_scan_tasks}" in explain_output

    result = df.to_pydict()

    # Verify we got the expected number of rows
    assert len(result["vector"]) == limit_size
    assert len(result["big_int"]) == limit_size

    # Verify the data is ordered correctly (should get first N rows)
    expected_big_ints = list(range(limit_size))
    assert result["big_int"] == expected_big_ints


def test_lancedb_with_version(lance_dataset_path):
    df = daft.read_lance(lance_dataset_path, version=1)
    assert df.to_pydict() == data

    # test pushdown filters with limit and projection
    def test_lancedb_read_pushdown(lance_dataset_path, capsys):
        df = daft.read_lance(lance_dataset_path)
        df = daft.sql("SELECT vector, lat + 1 as lat_plus_1 FROM df where  long < 3 limit 1")
        df.explain(show_all=True)
        captured = capsys.readouterr()
        explain_output = captured.out

        assert "Pushdowns: {projection: [vector, lat], filter: col(long) < lit(3), limit: 1}" in explain_output
        assert "Limit: 1" in explain_output

        result = df.to_pydict()
        assert len(result["vector"]) == 1

        df = daft.read_lance(lance_dataset_path)
        df = df.select("vector", "lat")
        assert df.to_pydict() == {"vector": data["vector"], "lat": data["lat"]}

        # multi filter
        daft.context.set_planning_config(enable_strict_filter_pushdown=True)

        df = daft.read_lance(lance_dataset_path)
        df = daft.sql("SELECT vector, lat + 1 as lat_plus_1 FROM df where  lat is not null  and big_int in (1, 2, 3)")
        df.explain(show_all=True)
        captured = capsys.readouterr()
        explain_output = captured.out
        physical_plan_start = explain_output.find("== Physical Plan ==")
        if physical_plan_start != -1:
            physical_plan_output = explain_output[physical_plan_start:]

            filter_count = physical_plan_output.count("Filter:")
            scan_source_count = physical_plan_output.count("ScanTaskSource:")

            assert (
                filter_count == 0 or filter_count == scan_source_count
            ), f"Physical plan contains {filter_count} Filter nodes and {scan_source_count} ScanTaskSource nodes, which is not expected"


def test_lancedb_read_parallelism_fragment_merging(large_lance_dataset_path):
    """Test parallelism parameter reduces scan tasks by merging fragments."""
    df_no_fragment_group = daft.read_lance(large_lance_dataset_path)
    assert len(lance.dataset(large_lance_dataset_path).get_fragments()) == df_no_fragment_group.num_partitions()

    df = daft.read_lance(large_lance_dataset_path, fragment_group_size=3)
    df.explain(show_all=True)
    assert df.num_partitions() == 4  # 10 fragments, group size 3 -> 4 scan tasks

    result = df.to_pydict()
    assert len(result["vector"]) == 10000
    assert len(result["big_int"]) == 10000


class TestLanceDBCountPushdown:
    tmp_data = {
        "a": ["a", "b", "c", "d", "e", None],
        "b": [1, None, 3, None, 5, 6],
        "c": [1, 2, 3, 4, 5, None],
    }

    @pytest.fixture(scope="function")
    def dataset_path(self, tmp_path_factory):
        tmp_dir = tmp_path_factory.mktemp("lance")
        lance.write_dataset(pa.Table.from_pydict(self.tmp_data), tmp_dir)
        yield str(tmp_dir)

    def test_count_all_pushdown(self, dataset_path, capsys):
        """Test count(*) pushdown with CountMode.All."""
        df = daft.read_lance(dataset_path).count()

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "Pushdowns: {projection: [b], aggregation: count(col(b), All)}" in actual.out
        assert "daft.io.lance.lance_scan:_lancedb_count_result_function" in actual.out

        result = df.to_pydict()
        assert result == {"count": [6]}

    def test_count_column_no_pushdown(self, dataset_path, capsys):
        """Test count(column) does not use pushdown as it's not a count(*)."""
        df = daft.read_lance(dataset_path).count("a")

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "Pushdowns: {projection: [a], aggregation: count(col(a), All)}" not in actual.out
        assert "daft.io.lance.lance_scan:_lancedb_count_result_function" not in actual.out

        result = df.to_pydict()
        assert result == {"a": [5]}

    def test_count_pushdown_with_select(self, dataset_path, capsys):
        """Test count(*) pushdown after select operation."""
        df = daft.read_lance(dataset_path).select("b").count()

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "Pushdowns: {projection: [b], aggregation: count(col(b), All)}" in actual.out
        assert "daft.io.lance.lance_scan:_lancedb_count_result_function" in actual.out

        result = df.to_pydict()
        assert result == {"count": [6]}

    def test_count_with_filter_pushdown(self, dataset_path, capsys):
        """Test count(*) uses filter+count pushdown when filter is present."""
        daft.context.set_planning_config(enable_strict_filter_pushdown=True)
        df = daft.read_lance(dataset_path).filter(col("b").is_null()).count()

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "daft.io.lance.lance_scan:_lancedb_count_result_function" in actual.out
        assert "Filter pushdown = is_null(col(b))" in actual.out
        assert "Aggregation pushdown = count(col(b), All)" in actual.out

        result = df.to_pydict()
        assert result == {"count": [2]}

    def test_count_with_or_filter_pushdown(self, dataset_path, capsys):
        """Test count(*) uses filter+count pushdown when filter is present."""
        daft.context.set_planning_config(enable_strict_filter_pushdown=True)
        df = daft.read_lance(dataset_path).filter(col("b").is_null() | col("c").is_null()).count()

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "daft.io.lance.lance_scan:_lancedb_count_result_function" in actual.out
        assert "Filter pushdown = is_null(col(b)) | is_null(col(c))" in actual.out
        assert "Aggregation pushdown = count(col(b), All)" in actual.out

        result = df.to_pydict()
        assert result == {"count": [3]}

    def test_count_with_and_filter_pushdown(self, dataset_path, capsys):
        """Test count(*) with complex filter conditions for filter+count pushdown."""
        daft.context.set_planning_config(enable_strict_filter_pushdown=True)

        df = daft.read_lance(dataset_path).filter((col("c") > 3) & ~col("b").is_null()).count()

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "daft.io.lance.lance_scan:_lancedb_count_result_function" in actual.out
        assert "Aggregation pushdown" in actual.out
        assert "Filter pushdown" in actual.out

        result = df.to_pydict()
        assert result == {"count": [1]}

    def test_count_with_filter_and_select_pushdown(self, dataset_path, capsys):
        """Test count(*) with both filter and select operations for filter+count pushdown."""
        daft.context.set_planning_config(enable_strict_filter_pushdown=True)
        df = daft.read_lance(dataset_path).filter(~col("b").is_null()).select("a", "b").count()

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "daft.io.lance.lance_scan:_lancedb_count_result_function" in actual.out

        result = df.to_pydict()
        assert result == {"count": [4]}

    def test_edge_case_empty_dataset(self, tmp_path_factory, capsys):
        """Test count pushdown on an empty dataset."""
        tmp_dir = tmp_path_factory.mktemp("empty_lance_table")
        empty_data = {"a": [], "b": []}
        lance.write_dataset(pa.Table.from_pydict(empty_data), tmp_dir)

        df = daft.read_lance(str(tmp_dir)).count()

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "Pushdowns: {projection: [a], aggregation: count(col(a), All)}" in actual.out
        assert "daft.io.lance.lance_scan:_lancedb_count_result_function" in actual.out

        result = df.to_pydict()
        assert result == {"count": [0]}

    def test_count_1(self, dataset_path, capsys):
        """Test count(*) pushdown with CountMode.All."""
        df = daft.read_lance(dataset_path).count(1)

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "Pushdowns: {projection: [b], aggregation: count(col(b), All)}" in actual.out
        assert "daft.io.lance.lance_scan:_lancedb_count_result_function" in actual.out

        result = df.to_pydict()
        assert result == {"count": [6]}


@pytest.mark.parametrize("enable_strict_filter_pushdown", [True, False])
def test_lancedb_filter_then_limit_behavior(lance_dataset_path, enable_strict_filter_pushdown):
    """Ensure filter is applied before limit for Lance reads."""
    daft.context.set_planning_config(enable_strict_filter_pushdown=enable_strict_filter_pushdown)
    df = daft.read_lance(lance_dataset_path)

    result1 = df.filter("big_int = 1").limit(1).to_pydict()
    assert result1 == {"vector": [[1.1, 1.2]], "lat": [45.5], "long": [-122.7], "big_int": [1]}

    result2 = df.filter("big_int = 2").limit(1).to_pydict()
    assert result2 == {"vector": [[0.2, 1.8]], "lat": [40.1], "long": [-74.1], "big_int": [2]}

    result3 = df.filter("big_int = 2").limit(2).to_pydict()
    assert result3 == {"vector": [[0.2, 1.8]], "lat": [40.1], "long": [-74.1], "big_int": [2]}


def test_lancedb_limit_with_filter_and_fragment_grouping_single_task(large_lance_dataset_path):
    """Validate filter+limit correctness when fragment grouping is enabled."""
    df = daft.read_lance(large_lance_dataset_path, fragment_group_size=4)
    df = df.filter("big_int = 999").limit(1).select("big_int")

    result = df.to_pydict()
    assert result == {"big_int": [999]}
