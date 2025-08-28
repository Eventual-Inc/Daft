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


def test_lancedb_read_limit(lance_dataset_path):
    df = daft.read_lance(lance_dataset_path)
    df = df.limit(1)
    df = df.select("vector")
    assert df.to_pydict() == {"vector": data["vector"][:1]}


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

    def test_count_no_pushdown_with_filter(self, dataset_path, capsys):
        """Test count(*) does not use pushdown when filter is present."""
        df = daft.read_lance(dataset_path).filter(col("b").is_null()).count()

        _ = capsys.readouterr()
        df.explain(True)
        actual = capsys.readouterr()
        assert "Pushdowns: {projection: [a], aggregation: count(col(a), All)}" not in actual.out
        assert "daft.io.lance.lance_scan:_lancedb_count_result_function" not in actual.out

        result = df.to_pydict()
        assert result == {"count": [2]}

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
