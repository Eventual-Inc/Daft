from __future__ import annotations

import pytest

import daft
from daft.dependencies import pa


@pytest.fixture(scope="function")
def lance_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("lance_update_evolution")
    yield str(tmp_dir)


def test_update_evolution_rowid(lance_dataset_path):
    # Dataset with two fragments
    data1 = {
        "id": [1, 2],
        "value": [10, 20],
    }
    data2 = {
        "id": [3, 4],
        "value": [30, 40],
    }

    df1 = daft.from_pydict(data1)
    df2 = daft.from_pydict(data2)
    df1.write_lance(lance_dataset_path, mode="create")
    df2.write_lance(lance_dataset_path, mode="append")

    # Read with _rowid and fragment_id enabled
    df_loaded = daft.read_lance(
        lance_dataset_path,
        default_scan_options={"with_row_id": True},
        include_fragment_id=True,
    )
    assert "fragment_id" in df_loaded.column_names
    assert "_rowid" in df_loaded.column_names

    pa_schema = df_loaded.schema().to_pyarrow_schema()
    assert pa_schema.field("fragment_id").type == pa.int64()

    # Prepare updates: bump value by +100 for all rows using _rowid as join key
    df_update = (
        df_loaded.select("_rowid", "fragment_id", "value")
        .with_column("value", daft.col("value") + 100)
        .select("_rowid", "fragment_id", "value")
    )

    daft.io.lance.update_columns_df(
        df_update,
        lance_dataset_path,
        read_columns=["_rowid", "value"],
        batch_size=1024,
    )

    df_after = daft.read_lance(lance_dataset_path)
    out = df_after.select("id", "value").to_pydict()

    assert out["id"] == [1, 2, 3, 4]
    assert out["value"] == [110, 120, 130, 140]


def test_update_evolution_business_key(lance_dataset_path):
    # Dataset with stable business key id
    data1 = {
        "id": [1, 2],
        "score": [10.0, 20.0],
    }
    data2 = {
        "id": [3, 4],
        "score": [30.0, 40.0],
    }

    df1 = daft.from_pydict(data1)
    df2 = daft.from_pydict(data2)
    df1.write_lance(lance_dataset_path, mode="create")
    df2.write_lance(lance_dataset_path, mode="append")

    df_loaded = daft.read_lance(
        lance_dataset_path,
        default_scan_options={"with_row_id": True},
        include_fragment_id=True,
    )
    assert "fragment_id" in df_loaded.column_names

    # Reader contains {id} + updated score (+ fragment_id for grouping)
    df_update = (
        df_loaded.select("id", "fragment_id", "score")
        .with_column("score", daft.col("score") * 10.0)
        .select("id", "fragment_id", "score")
    )

    daft.io.lance.update_columns_df(
        df_update,
        lance_dataset_path,
        read_columns=["id", "score"],
        batch_size=1024,
        left_on="id",
        right_on="id",
    )

    df_after = daft.read_lance(lance_dataset_path)
    out = df_after.select("id", "score").to_pydict()

    assert out["id"] == [1, 2, 3, 4]
    # All scores should be multiplied by 10
    assert out["score"] == [100.0, 200.0, 300.0, 400.0]
