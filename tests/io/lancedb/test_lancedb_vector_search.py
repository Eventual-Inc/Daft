from __future__ import annotations

import inspect
import io

import lance
import pyarrow as pa
import pytest

import daft
from daft import col


def build_single_fragment_dataset(tmp_path_factory) -> str:
    tmp_dir = tmp_path_factory.mktemp("lance_vector_single")

    # Fixed-size list column so Lance treats it as a vector column.
    vector_type = pa.list_(pa.float32(), 2)
    vectors = pa.array([[0.0, 0.0], [1.0, 0.0], [0.0, 1.0]], type=vector_type)
    ids = pa.array([0, 1, 2], type=pa.int64())

    table = pa.table({"id": ids, "vector": vectors})
    lance.write_dataset(table, tmp_dir)
    return str(tmp_dir)


def build_multi_fragment_dataset(tmp_path_factory) -> str:
    tmp_dir = tmp_path_factory.mktemp("lance_vector_multi")

    vector_type = pa.list_(pa.float32(), 2)

    # Fragment 1: far away points.
    vectors1 = pa.array([[100.0, 0.0], [101.0, 0.0]], type=vector_type)
    ids1 = pa.array([0, 1], type=pa.int64())
    tbl1 = pa.table({"id": ids1, "vector": vectors1})
    lance.write_dataset(tbl1, tmp_dir)

    # Fragment 2: contains the true nearest neighbor to the origin.
    vectors2 = pa.array([[0.1, 0.0], [0.0, 2.0]], type=vector_type)
    ids2 = pa.array([2, 3], type=pa.int64())
    tbl2 = pa.table({"id": ids2, "vector": vectors2})
    lance.write_dataset(tbl2, tmp_dir, mode="append")

    return str(tmp_dir)


def build_metric_dataset(tmp_path_factory) -> str:
    tmp_dir = tmp_path_factory.mktemp("lance_vector_metric")

    vector_type = pa.list_(pa.float32(), 2)
    vectors = pa.array([[1.0, 0.0], [0.0, 1.0], [-1.0, 0.0]], type=vector_type)
    ids = pa.array([0, 1, 2], type=pa.int64())

    table = pa.table({"id": ids, "vector": vectors})
    lance.write_dataset(table, tmp_dir)
    return str(tmp_dir)


def build_prefilter_dataset(tmp_path_factory) -> str:
    tmp_dir = tmp_path_factory.mktemp("lance_vector_prefilter")

    vector_type = pa.list_(pa.float32(), 2)
    vectors = pa.array([[0.0, 0.0], [0.2, 0.0], [2.0, 2.0]], type=vector_type)
    ids = pa.array([0, 1, 2], type=pa.int64())
    groups = pa.array([0, 1, 1], type=pa.int64())

    table = pa.table({"id": ids, "group": groups, "vector": vectors})
    lance.write_dataset(table, tmp_dir)
    return str(tmp_dir)


@pytest.mark.parametrize("k", [1, 2])
def test_nearest_single_fragment(tmp_path_factory, k) -> None:
    dataset_path = build_single_fragment_dataset(tmp_path_factory)

    # Query is closest to [1.0, 0.0] (id=1).
    query = pa.array([1.0, 0.0], type=pa.float32())
    nearest = {"column": "vector", "q": query, "k": k}

    df = daft.read_lance(dataset_path, default_scan_options={"nearest": nearest})
    result = df.select("id").to_pydict()

    # Expected: id=1 is closest (dist 0), id=0 is second (dist 1), id=2 is third (dist sqrt(2))
    expected_ids = [1, 0][:k]
    assert len(result["id"]) == k
    assert set(result["id"]) == set(expected_ids)


def test_nearest_multi_fragment_global_k1(tmp_path_factory) -> None:
    dataset_path = build_multi_fragment_dataset(tmp_path_factory)

    # Global nearest to the origin is id=2 (vector [0.1, 0.0]).
    query = pa.array([0.0, 0.0], type=pa.float32())
    nearest = {"column": "vector", "q": query, "k": 1}

    df = daft.read_lance(dataset_path, default_scan_options={"nearest": nearest})
    result = df.select("id").to_pydict()

    assert result["id"] == [2]


def test_nearest_global_single_scan_task(tmp_path_factory) -> None:
    """Ensure we plan a single scan task when nearest search is configured."""
    dataset_path = build_multi_fragment_dataset(tmp_path_factory)

    query = pa.array([0.0, 0.0], type=pa.float32())
    nearest = {"column": "vector", "q": query, "k": 1}

    df = daft.read_lance(dataset_path, default_scan_options={"nearest": nearest})

    string_io = io.StringIO()
    df.explain(show_all=True, file=string_io)
    explain_output = string_io.getvalue()

    assert "Num Scan Tasks = 1" in explain_output


def test_nearest_metric_cosine_k1(tmp_path_factory) -> None:
    dataset_path = build_metric_dataset(tmp_path_factory)

    # For cosine distance, [0.0, 1.0] is closest to the [0.0, 1.0] row (id=1).
    query = pa.array([0.0, 1.0], type=pa.float32())
    nearest = {"column": "vector", "q": query, "k": 1, "metric": "cosine"}

    df = daft.read_lance(dataset_path, default_scan_options={"nearest": nearest})
    result = df.select("id").to_pydict()

    assert result["id"] == [1]


def test_nearest_params_passthrough(tmp_path_factory) -> None:
    """Test that extra parameters like nprobes, refine_factor are passed without error."""
    dataset_path = build_single_fragment_dataset(tmp_path_factory)
    query = pa.array([1.0, 0.0], type=pa.float32())

    # refine_factor and nprobes usually require index or are ignored, but shouldn't crash
    nearest = {
        "column": "vector",
        "q": query,
        "k": 1,
        "nprobes": 10,
        "refine_factor": 5,
    }
    df = daft.read_lance(dataset_path, default_scan_options={"nearest": nearest})
    result = df.select("id").to_pydict()
    assert result["id"] == [1]


def test_nearest_with_index(tmp_path_factory) -> None:
    tmp_dir = tmp_path_factory.mktemp("lance_vector_index")
    # Need enough data for index.
    vector_type = pa.list_(pa.float32(), 2)
    # 3 distinct points, repeated 100 times
    vectors = pa.array([[0.0, 0.0], [1.0, 0.0], [0.0, 1.0]] * 100, type=vector_type)
    ids = pa.array(range(300), type=pa.int64())
    table = pa.table({"id": ids, "vector": vectors})
    lance.write_dataset(table, tmp_dir)

    ds = lance.dataset(str(tmp_dir))
    # Create a small index
    try:
        ds.create_index("vector", "IVF_PQ", num_partitions=2, num_sub_vectors=1)
    except Exception:
        pytest.skip("Could not create index (lance version or dataset size issue)")

    query = pa.array([0.0, 0.0], type=pa.float32())

    # 1. Use index (default or explicit)
    nearest_idx = {"column": "vector", "q": query, "k": 1, "use_index": True}
    df_idx = daft.read_lance(str(tmp_dir), default_scan_options={"nearest": nearest_idx})
    res_idx = df_idx.select("id").to_pydict()
    assert len(res_idx["id"]) == 1
    # Check if the result is valid (distance should be 0, so id % 3 == 0)
    assert res_idx["id"][0] % 3 == 0

    # 2. No index
    nearest_no_idx = {"column": "vector", "q": query, "k": 1, "use_index": False}
    df_no_idx = daft.read_lance(str(tmp_dir), default_scan_options={"nearest": nearest_no_idx})
    res_no_idx = df_no_idx.select("id").to_pydict()
    assert len(res_no_idx["id"]) == 1
    assert res_no_idx["id"][0] % 3 == 0


def test_nearest_with_projection(tmp_path_factory) -> None:
    dataset_path = build_single_fragment_dataset(tmp_path_factory)
    query = pa.array([1.0, 0.0], type=pa.float32())
    nearest = {"column": "vector", "q": query, "k": 1}

    df = daft.read_lance(dataset_path, default_scan_options={"nearest": nearest})
    # Select vector column too
    result = df.select("id", "vector").to_pydict()

    assert result["id"] == [1]
    assert result["vector"] == [[1.0, 0.0]]


def test_nearest_distance_range(tmp_path_factory) -> None:
    # distance_range is only available in newer versions of Lance. Skip the test
    # if the installed version does not support it to keep the suite compatible
    # with older environments.
    import importlib

    try:
        lance_dataset_module = importlib.import_module("lance.dataset")
    except Exception:
        pytest.skip("lance.dataset module not available; skipping distance_range test")

    ScannerBuilder = getattr(lance_dataset_module, "ScannerBuilder", None)
    if ScannerBuilder is None:
        pytest.skip("ScannerBuilder not available; distance_range not supported")

    nearest_sig = inspect.signature(ScannerBuilder.nearest)
    if "distance_range" not in nearest_sig.parameters:
        pytest.skip("distance_range is not supported by the installed lance version")

    dataset_path = build_multi_fragment_dataset(tmp_path_factory)

    query = pa.array([0.0, 0.0], type=pa.float32())
    nearest = {
        "column": "vector",
        "q": query,
        "k": 10,
        "distance_range": (0.0, 0.5),
    }

    df = daft.read_lance(dataset_path, default_scan_options={"nearest": nearest})
    ids = df.select("id").to_pydict()["id"]

    # Only id=2 lies within the requested distance range of the origin.
    assert ids == [2]


def test_nearest_with_prefilter_true(tmp_path_factory) -> None:
    dataset_path = build_prefilter_dataset(tmp_path_factory)

    query = pa.array([0.0, 0.0], type=pa.float32())
    nearest = {"column": "vector", "q": query, "k": 1}

    # Ensure filter is pushed down so Lance can honor prefilter semantics.
    daft.context.set_planning_config(enable_strict_filter_pushdown=True)

    df = daft.read_lance(
        dataset_path,
        default_scan_options={"nearest": nearest, "prefilter": True},
    )
    filtered = df.where(col("group") == 1)
    result = filtered.select("id", "group").to_pydict()

    # With prefilter=True, Lance should restrict to group==1 before running nearest
    # so the closest row in that group (id=1) is returned.
    assert result["id"] == [1]
    assert result["group"] == [1]


def test_nearest_with_prefilter_false(tmp_path_factory) -> None:
    dataset_path = build_prefilter_dataset(tmp_path_factory)

    query = pa.array([0.0, 0.0], type=pa.float32())
    nearest = {"column": "vector", "q": query, "k": 1}

    daft.context.set_planning_config(enable_strict_filter_pushdown=True)

    df = daft.read_lance(
        dataset_path,
        default_scan_options={"nearest": nearest, "prefilter": False},
    )
    filtered = df.where(col("group") == 1)
    result = filtered.select("id", "group").to_pydict()

    # With prefilter=False, Lance first runs nearest globally (id=0, group=0) and
    # then applies the filter, which can yield fewer than K results (or empty).
    assert result["id"] == []
    assert result["group"] == []
