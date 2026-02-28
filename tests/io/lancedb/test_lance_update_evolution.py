from __future__ import annotations

import pytest
import json
import os

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

    daft.io.lance.update_columns(
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

    daft.io.lance.update_columns(
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


def test_update_single_column(lance_dataset_path):
    data1 = {"id": [1], "val": [10], "other": ["a"]}
    data2 = {"id": [2, 3], "val": [20, 30], "other": ["b", "c"]}
    daft.from_pydict(data1).write_lance(lance_dataset_path, mode="create")
    daft.from_pydict(data2).write_lance(lance_dataset_path, mode="append")

    df = daft.read_lance(lance_dataset_path, default_scan_options={"with_row_id": True}, include_fragment_id=True)
    # Ensure we have multiple fragments
    assert len(set(df.collect().to_pydict()["fragment_id"])) > 1

    df_update = df.with_column("val", daft.col("val") + 1).select("_rowid", "fragment_id", "val")
    daft.io.lance.update_columns(df_update, lance_dataset_path)

    result = daft.read_lance(lance_dataset_path).sort("id").to_pydict()
    assert result["val"] == [11, 21, 31]
    assert result["other"] == ["a", "b", "c"]


def test_update_multiple_columns(lance_dataset_path):
    data1 = {"id": [1], "val1": [10], "val2": [1.0]}
    data2 = {"id": [2, 3], "val1": [20, 30], "val2": [2.0, 3.0]}
    daft.from_pydict(data1).write_lance(lance_dataset_path, mode="create")
    daft.from_pydict(data2).write_lance(lance_dataset_path, mode="append")

    df = daft.read_lance(lance_dataset_path, default_scan_options={"with_row_id": True}, include_fragment_id=True)
    # Ensure we have multiple fragments
    assert len(set(df.collect().to_pydict()["fragment_id"])) > 1

    df_update = (
        df.with_column("val1", daft.col("val1") + 1)
        .with_column("val2", daft.col("val2") * 2)
        .select("_rowid", "fragment_id", "val1", "val2")
    )
    daft.io.lance.update_columns(df_update, lance_dataset_path)

    result = daft.read_lance(lance_dataset_path).sort("id").to_pydict()
    assert result["val1"] == [11, 21, 31]
    assert result["val2"] == [2.0, 4.0, 6.0]


def test_update_from_external_json_join(lance_dataset_path):
    """
    Test scenario:
    1. An existing Lance dataset (target) with a business key 'item_id'.
    2. An external JSON file (source) containing updates for some items.
    3. We need to JOIN the target (to get fragment_id) with the source (to get new values).
    4. Apply updates to the Lance dataset using the business key.
    """
    
    # 1. Setup Lance Table (Target)
    # Create 2 Fragments to ensure distributed/fragment-aware logic works
    data1 = {"item_id": [1, 2], "price": [100, 200], "category": ["A", "B"]}
    data2 = {"item_id": [3, 4], "price": [300, 400], "category": ["A", "C"]}
    daft.from_pydict(data1).write_lance(lance_dataset_path, mode="create")
    daft.from_pydict(data2).write_lance(lance_dataset_path, mode="append")
    
    # 2. Setup External JSON Source (Updates)
    # Updates for item_id 2 and 3. Item 1 and 4 are untouched.
    # Item 5 is new (should be ignored by inner join, or handled if we wanted upsert)
    updates = [
        {"item_id": 2, "new_price": 250, "new_category": "B_new"},
        {"item_id": 3, "new_price": 350, "new_category": "A_new"},
        {"item_id": 5, "new_price": 500, "new_category": "D"} # Non-existent key in target
    ]
    json_path = os.path.join(lance_dataset_path, "updates.json")
    with open(json_path, "w") as f:
        for row in updates:
            f.write(json.dumps(row) + "\n")
            
    # 3. Read Data
    # Read Lance to get fragment_ids (required for update_columns)
    # We only need the join key and the metadata columns
    target_df = daft.read_lance(
        lance_dataset_path, 
        include_fragment_id=True
    ).select("item_id", "fragment_id")
    
    # Read Updates
    source_df = daft.read_json(json_path)
    
    # 4. Join to associate Fragment ID with Updates
    # We join on business key "item_id"
    # Inner join: only update rows that exist in both target and source
    update_plan = target_df.join(source_df, on="item_id")
    
    # 5. Prepare Update DataFrame
    # Must contain:
    # - fragment_id (for Lance to know which fragment to rewrite)
    # - Join Key (item_id, for Merge logic within fragment)
    # - Updated Columns (price, category) - mapped from new_* columns
    df_for_update = (
        update_plan
        .with_column("price", daft.col("new_price"))
        .with_column("category", daft.col("new_category"))
        .select("fragment_id", "item_id", "price", "category")
    )
    
    # 6. Execute Update
    # left_on/right_on refer to the merge key within the fragment (business key)
    daft.io.lance.update_columns(
        df_for_update,
        lance_dataset_path,
        left_on="item_id",
        right_on="item_id"
    )
    
    # 7. Verify Results
    result = daft.read_lance(lance_dataset_path).sort("item_id").to_pydict()
    
    # Check item_ids (should be 1, 2, 3, 4 - item 5 was ignored)
    assert result["item_id"] == [1, 2, 3, 4]
    
    # Check prices
    # 1: 100 (original)
    # 2: 250 (updated)
    # 3: 350 (updated)
    # 4: 400 (original)
    assert result["price"] == [100, 250, 350, 400]
    
    # Check categories
    assert result["category"] == ["A", "B_new", "A_new", "C"]
