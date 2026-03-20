from __future__ import annotations

import lance
import pyarrow as pa
import pytest

import daft
from daft import col

TABLE_NAME = "my_table"
data = {"vector": [[1.1, 1.2], [0.2, 1.8]], "lat": [45.5, 40.1], "long": [-122.7, -74.1], "big_int": [1, 2]}


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
    df = daft.read_lance(uri=lance_dataset_path, version=1)
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

            assert filter_count == 0 or filter_count == scan_source_count, (
                f"Physical plan contains {filter_count} Filter nodes and {scan_source_count} ScanTaskSource nodes, which is not expected"
            )


def test_lancedb_read_parallelism_fragment_merging(large_lance_dataset_path):
    """Test parallelism parameter reduces scan tasks by merging fragments."""
    df = daft.read_lance(uri=large_lance_dataset_path, fragment_group_size=3)
    result = df.to_pydict()
    assert len(result["vector"]) == 10000
    assert len(result["big_int"]) == 10000


def test_lancedb_read_filter_passthrough(tmp_path):
    """Test passing raw SQL filter string to Lance via default_scan_options."""
    pytest.importorskip("shapely")
    import lance
    from shapely.geometry import Point

    # Create dataset with points
    # Point 0: (0, 0)
    # Point 1: (10, 10)
    # Point 2: (20, 20)
    points_list = [Point(i * 10, i * 10).wkb for i in range(3)]

    schema = pa.schema([pa.field("point", pa.binary()), pa.field("id", pa.int32())])

    table = pa.Table.from_pydict({"point": points_list, "id": list(range(3))}, schema=schema)

    dataset_path = str(tmp_path / "test_geo_filter_passthrough.lance")
    lance.write_dataset(table, dataset_path)

    # Test: Pass a raw SQL filter string to Lance via default_scan_options
    # We use a simple filter first to verify the mechanism works
    filter_str = "id >= 1"

    df = daft.read_lance(dataset_path, default_scan_options={"filter": filter_str})

    res = df.to_pydict()

    assert len(res["id"]) == 2
    assert sorted(res["id"]) == [1, 2]
    assert 0 not in res["id"]
    assert 1 in res["id"]
    assert 2 in res["id"]


def test_lancedb_geo_projection_and_filter(tmp_path):
    """Test LanceDB read with Geo projection and filter via default_scan_options."""
    import lance
    from packaging import version

    if version.parse(lance.__version__) < version.parse("1.0.0"):
        pytest.skip("LanceDB version must be >= 1.0.0 for Geo support")

    try:
        import numpy as np
        from geoarrow.pyarrow import linestring, point
    except ImportError:
        pytest.skip("geoarrow-pyarrow not installed")

    np.random.seed(42)
    num_rows = 10000
    # Points
    x_coords = np.random.rand(num_rows) * 100
    y_coords = np.random.rand(num_rows) * 100

    # LineStrings
    # Create simple linestrings. Each linestring has 2 points.
    # We need 2 * num_rows coordinates for linestrings
    ls_x = np.random.randn(num_rows * 2) * 100
    ls_y = np.random.randn(num_rows * 2) * 100

    # Force first linestring to intersect with 'LINESTRING ( 2 0, 0 2 )'
    # Row 0: Linestring (0,0)->(2,2). Intersects query at (1,1).
    ls_x[0], ls_y[0] = 0.0, 0.0
    ls_x[1], ls_y[1] = 2.0, 2.0
    # Set Row 0 point to (1,1) so distance to linestring is 0
    x_coords[0], y_coords[0] = 1.0, 1.0

    # Row 1: Linestring (100,100)->(102,102). Does NOT intersect.
    ls_x[2], ls_y[2] = 100.0, 100.0
    ls_x[3], ls_y[3] = 102.0, 102.0

    # Move all other random data far away to ensure no accidental intersections
    # Points from index 1 onwards (Row 1+)
    x_coords[1:] += 1000.0
    y_coords[1:] += 1000.0
    # Linestrings from index 4 onwards (Row 2+)
    ls_x[4:] += 1000.0
    ls_y[4:] += 1000.0

    points_2d = point().from_geobuffers(None, x_coords, y_coords)

    # Offsets: 0, 2, 4, ...
    line_offsets = np.arange(num_rows + 1, dtype=np.int32) * 2

    linestrings_2d = linestring().from_geobuffers(None, line_offsets, ls_x, ls_y)

    schema = pa.schema(
        [
            pa.field("point", points_2d.type),
            pa.field("linestring", linestrings_2d.type),
        ]
    )

    table = pa.Table.from_arrays([points_2d, linestrings_2d], schema=schema)
    dataset_path = str(tmp_path / "test_geo_udf_distance.lance")
    lance.write_dataset(table, dataset_path)

    # Read with Daft
    # We expect 'distance' column in the result
    df = daft.read_lance(
        dataset_path,
        default_scan_options={
            "columns": {"distance": "st_distance(point, linestring)"},
            "filter": "st_intersects(linestring, st_geomfromtext('LINESTRING ( 2 0, 0 2 )'))",
            "with_row_id": True,
        },
    )

    # Verify schema has 'distance'
    print(f"Daft Schema: {df.schema()}")

    # Execute
    res = df.to_pydict()

    # We don't know exactly how many rows will match random data, but we can check structure
    assert "distance" in res
    assert "point" not in res  # Should be projected out
    assert "linestring" not in res  # Should be projected out

    # Check if we got any rows (might be 0 if random data doesn't intersect)
    print(f"Result rows: {len(res['distance'])}")

    # We forced exactly one intersection (Row 0)
    # All other rows are shifted far away
    assert len(res["distance"]) == 1

    # Verify the distance calculation for the matching row
    # Point (1,1) is on Linestring (0,0)->(2,2), so distance should be 0.0
    # Use approx for float comparison
    assert abs(res["distance"][0]) < 1e-6


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

        df = daft.read_lance(tmp_dir).count()

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
    df = daft.read_lance(uri=large_lance_dataset_path, fragment_group_size=4)
    df = df.filter("big_int = 999").limit(1).select("big_int")

    result = df.to_pydict()
    assert result == {"big_int": [999]}
