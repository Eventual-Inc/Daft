from __future__ import annotations

import lance
import pyarrow as pa
import pytest

import daft
from daft import col

TABLE_NAME = "my_table"
data = {
    "vector": [[1.1, 1.2], [0.2, 1.8]],
    "lat": [45.5, 40.1],
    "long": [-122.7, -74.1],
}

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
    df = df.where(df["lat"] > 45)
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

        df.explain(True)
        actual = capsys.readouterr()

        expected = """* Project: col(0: a) as count
|   Stats = { Approx num rows = 1, Approx size bytes = 8 B, Accumulated selectivity
|     = 1.00 }
|
* ScanTaskSource:
|   Num Scan Tasks = 1
|   Estimated Scan Bytes = 0
|   Pushdowns: {projection: [a], aggregation: count(col(a), All)}
|   Schema: {a#UInt64}
|   Scan Tasks: [
|   {daft.io.lance.lance_scan:_lancedb_count_result_function}
|   ]
|   Stats = { Approx num rows = 1, Approx size bytes = 8 B, Accumulated selectivity
|     = 1.00 }
"""
        assert expected in actual.out

        result = df.to_pydict()
        assert result == {"count": [6]}

    def test_count_column_no_pushdown(self, dataset_path, capsys):
        """Test count(column) does not use pushdown as it's not a count(*)."""
        df = daft.read_lance(dataset_path).count("a")

        df.explain(True)
        actual = capsys.readouterr()

        expected = """* Aggregate: count(col(0: a), Valid)
|   Stats = { Approx num rows = 1, Approx size bytes = 0 B, Accumulated selectivity
|     = 0.00 }
|
* ScanTaskSource:
|   Num Scan Tasks = 1
|   Estimated Scan Bytes = 0
|   Pushdowns: {projection: [a]}
|   Schema: {a#Utf8, b#Int64, c#Int64}
|   Scan Tasks: [
|   {daft.io.lance.lance_scan:_lancedb_table_factory_function}
|   ]
|   Stats = { Approx num rows = 0, Approx size bytes = 0 B, Accumulated selectivity
|     = 1.00 }
"""
        assert expected in actual.out

        result = df.to_pydict()
        assert result == {"a": [5]}

    def test_count_pushdown_select(self, dataset_path, capsys):
        """Test count(column, CountMode.Valid) does not use pushdown as it's not supported."""
        df = daft.read_lance(dataset_path).select("b").count()

        df.explain(True)
        actual = capsys.readouterr()

        expected = """* Project: col(0: b) as count
|   Stats = { Approx num rows = 1, Approx size bytes = 8 B, Accumulated selectivity
|     = 1.00 }
|
* ScanTaskSource:
|   Num Scan Tasks = 1
|   Estimated Scan Bytes = 0
|   Pushdowns: {projection: [b], aggregation: count(col(b), All)}
|   Schema: {b#UInt64}
|   Scan Tasks: [
|   {daft.io.lance.lance_scan:_lancedb_count_result_function}
|   ]
|   Stats = { Approx num rows = 1, Approx size bytes = 8 B, Accumulated selectivity
|     = 1.00 }
"""
        assert expected in actual.out

        result = df.to_pydict()
        assert result == {"count": [6]}

    def test_count_no_pushdown_filter(self, dataset_path, capsys):
        """Test count(column, CountMode.Null) does not use pushdown as it's not supported."""
        df = daft.read_lance(dataset_path).filter(col("b").is_null()).count()

        df.explain(True)
        actual = capsys.readouterr()

        expected = """* Project: col(0: a) as count
|   Stats = { Approx num rows = 1, Approx size bytes = 0 B, Accumulated selectivity
|     = 0.00 }
|
* Aggregate: count(col(0: a), All)
|   Stats = { Approx num rows = 1, Approx size bytes = 0 B, Accumulated selectivity
|     = 0.00 }
|
* ScanTaskSource:
|   Num Scan Tasks = 1
|   Estimated Scan Bytes = 0
|   Pushdowns: {projection: [a], filter: is_null(col(b))}
|   Schema: {a#Utf8, b#Int64, c#Int64}
|   Scan Tasks: [
|   {daft.io.lance.lance_scan:_lancedb_table_factory_function}
|   ]
|   Stats = { Approx num rows = 0, Approx size bytes = 0 B, Accumulated selectivity
|     = 0.05 }
"""
        assert expected in actual.out

        result = df.to_pydict()
        assert result == {"count": [2]}

    def test_edge_case_empty_dataset(self, tmp_path_factory, capsys):
        """Test count pushdown on an empty dataset."""
        tmp_dir = tmp_path_factory.mktemp("empty_lance_table")
        empty_data = {"a": [], "b": []}
        lance.write_dataset(pa.Table.from_pydict(empty_data), tmp_dir)

        df = daft.read_lance(str(tmp_dir)).count()

        df.explain(True)
        actual = capsys.readouterr()

        expected = """* Project: col(0: a) as count
|   Stats = { Approx num rows = 1, Approx size bytes = 8 B, Accumulated selectivity
|     = 1.00 }
|
* ScanTaskSource:
|   Num Scan Tasks = 1
|   Estimated Scan Bytes = 0
|   Pushdowns: {projection: [a], aggregation: count(col(a), All)}
|   Schema: {a#UInt64}
|   Scan Tasks: [
|   {daft.io.lance.lance_scan:_lancedb_count_result_function}
|   ]
|   Stats = { Approx num rows = 1, Approx size bytes = 8 B, Accumulated selectivity
|     = 1.00 }
"""
        assert expected in actual.out

        result = df.to_pydict()
        assert result == {"count": [0]}
