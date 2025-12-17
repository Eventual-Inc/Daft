from __future__ import annotations

import pytest

import daft
from daft.dependencies import pa


@pytest.fixture(scope="function")
def lance_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("lance_merge_evolution")
    yield str(tmp_dir)


def test_merge_evolution_rowaddr(lance_dataset_path):
    # Dataset with two fragments
    data1 = {
        "vector": [[1.1, 1.2], [0.2, 1.8]],
        "lat": [45.5, 40.1],
        "long": [-122.7, -74.1],
    }
    data2 = {
        "vector": [[2.1, 2.2], [2.2, 2.8]],
        "lat": [46.5, 41.1],
        "long": [-123.7, -75.1],
    }

    df1 = daft.from_pydict(data1)
    df2 = daft.from_pydict(data2)
    df1.write_lance(lance_dataset_path, mode="create")
    df2.write_lance(lance_dataset_path, mode="append")

    # Read with _rowaddr and fragment_id enabled
    df_loaded = daft.read_lance(
        lance_dataset_path, default_scan_options={"with_row_address": True}, include_fragment_id=True
    )
    assert "fragment_id" in df_loaded.column_names
    pa_schema = df_loaded.schema().to_pyarrow_schema()
    assert pa_schema.field("fragment_id").type == pa.int64()

    # Keep only {_rowaddr, fragment_id} + new columns
    df_subset = (
        df_loaded.select("lat", "fragment_id", "_rowaddr")
        .with_column("double_lat", daft.col("lat") * 2)
        .select("_rowaddr", "fragment_id", "double_lat")
    )

    # Merge mode column evolution by _rowaddr
    df_subset.write_lance(lance_dataset_path, mode="merge")

    # New column exists with correct values; row count unchanged
    df_after = daft.read_lance(lance_dataset_path)
    assert "double_lat" in df_after.column_names

    out = df_after.select("lat", "double_lat").to_pydict()
    lat_vals = out["lat"]
    doubled_vals = out["double_lat"]
    assert len(lat_vals) == len(doubled_vals)
    for a, b in zip(lat_vals, doubled_vals):
        assert pytest.approx(a * 2, rel=1e-6) == b

    assert df_after.count_rows() == df_loaded.count_rows()


def test_merge_evolution_business_key(lance_dataset_path):
    # Dataset with stable business key frame_key
    data1 = {
        "frame_key": [1, 2],
        "lat": [10.0, 20.0],
        "long": [1.0, 2.0],
        "vector": [[0.1, 0.2], [0.3, 0.4]],
    }
    data2 = {
        "frame_key": [2, 3],
        "lat": [30.0, 40.0],
        "long": [3.0, 4.0],
        "vector": [[0.5, 0.6], [0.7, 0.8]],
    }

    df1 = daft.from_pydict(data1)
    df2 = daft.from_pydict(data2)
    df1.write_lance(lance_dataset_path, mode="create")
    df2.write_lance(lance_dataset_path, mode="append")

    df_loaded = daft.read_lance(
        lance_dataset_path, default_scan_options={"with_row_address": True}, include_fragment_id=True
    )
    assert "fragment_id" in df_loaded.column_names
    schema = df_loaded.schema().to_pyarrow_schema()
    assert schema.field("fragment_id").type == pa.int64()

    # Reader contains {frame_key} + new column (+ fragment_id for grouping)
    df_subset = (
        df_loaded.select("frame_key", "fragment_id")
        .with_column("double_frame_key", daft.col("frame_key") * 10)
        .select("frame_key", "fragment_id", "double_frame_key")
    )

    # Merge by business key
    df_subset.write_lance(lance_dataset_path, mode="merge", left_on="frame_key", right_on="frame_key")

    df_after = daft.read_lance(lance_dataset_path)
    assert "double_frame_key" in df_after.column_names

    out = df_after.select("frame_key", "double_frame_key").to_pydict()
    fk_vals = out["frame_key"]
    doubled_vals = out["double_frame_key"]
    assert len(fk_vals) == len(doubled_vals)
    for a, b in zip(fk_vals, doubled_vals):
        assert pytest.approx(a * 10, rel=1e-6) == b

    assert df_after.count_rows() == df_loaded.count_rows()


def test_merge_columns_df_rowaddr(lance_dataset_path):
    # DF-driven per-fragment merging (merge_columns_df)
    base = {"vector": [[1.0, 1.1], [2.0, 2.1]], "lat": [11.0, 22.0], "long": [1.0, 2.0]}
    df1 = daft.from_pydict(base)
    df1.write_lance(lance_dataset_path, mode="create")

    df_loaded = daft.read_lance(
        lance_dataset_path, default_scan_options={"with_row_address": True}, include_fragment_id=True
    )
    df_subset = (
        df_loaded.select("lat", "fragment_id", "_rowaddr")
        .with_column("double_lat", daft.col("lat") * 2)
        .select("_rowaddr", "fragment_id", "double_lat")
    )

    daft.io.lance.merge_columns_df(
        df_subset,
        lance_dataset_path,
        read_columns=["_rowaddr", "double_lat"],
        batch_size=1024,
    )

    df_after = daft.read_lance(lance_dataset_path)
    out = df_after.select("lat", "double_lat").to_pydict()
    for a, b in zip(out["lat"], out["double_lat"]):
        assert pytest.approx(a * 2, rel=1e-6) == b
    assert df_after.count_rows() == df_loaded.count_rows()
