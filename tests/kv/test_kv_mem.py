from __future__ import annotations

import time

import pytest

import daft
from daft.expressions import col
from daft.functions.kv import kv_get_with_name
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray"
    or daft.context.get_context().daft_execution_config.use_legacy_ray_runner is True,
    reason="mem kv store tests are only supported on native runner",
)

# Common test data
EXPECTED_DATA = [
    {"item_id": 0, "embedding": [0.0, 0.1], "metadata": "m0"},
    {"item_id": 1, "embedding": [1.0, 1.1], "metadata": "m1"},
    {"item_id": 2, "embedding": [2.0, 2.2], "metadata": "m2"},
]


def setup_test_kv_store():
    """Set up a test KV store with common configuration."""
    timestamp = str(int(time.time() * 1000000))

    embeddings_kv = daft.set_kv("memory", name=f"embeddings_{timestamp}")
    metadata_kv = daft.set_kv("memory", name=f"metadata_{timestamp}")

    emb_alias = f"mem_embeddings_{timestamp}"
    meta_alias = f"mem_metadata_{timestamp}"
    daft.attach_kv(embeddings_kv, alias=emb_alias)
    daft.attach_kv(metadata_kv, alias=meta_alias)

    daft.set_kv(emb_alias)

    df = daft.from_pydict(
        {
            "item_id": [item["item_id"] for item in EXPECTED_DATA],
            "embedding": [item["embedding"] for item in EXPECTED_DATA],
            "metadata": [item["metadata"] for item in EXPECTED_DATA],
        }
    )

    # Put data into KV stores
    _ = df.select(daft.kv_put(embeddings_kv, col("item_id"), col("embedding")).alias("result")).collect()
    _ = df.select(daft.kv_put(metadata_kv, col("item_id"), col("metadata")).alias("result")).collect()

    return {
        "embeddings_kv": embeddings_kv,
        "metadata_kv": metadata_kv,
        "emb_alias": emb_alias,
        "meta_alias": meta_alias,
        "df": df,
        "timestamp": timestamp,
    }


def test_kv_final_execution_logic():
    kv_setup = setup_test_kv_store()

    e1 = kv_get_with_name("item_id", kv_setup["emb_alias"])
    e2 = kv_get_with_name("item_id", kv_setup["meta_alias"])
    merge = daft.func(lambda a, b: {**(a or {}), **(b or {})}, return_dtype=daft.DataType.python())
    df_out = kv_setup["df"].with_column("data", merge(e1, e2)).collect()

    pd = df_out.to_pydict()
    assert isinstance(pd["data"][0], dict)
    assert "embedding" in pd["data"][0]
    assert "metadata" in pd["data"][0]
    assert pd["data"][1]["embedding"] == [1.0, 1.1]
    assert pd["data"][2]["metadata"] == "m2"

    for i, expected in enumerate(EXPECTED_DATA):
        assert pd["data"][i]["embedding"] == expected["embedding"]
        assert pd["data"][i]["metadata"] == expected["metadata"]


def test_kv_get_selective_single_column():
    kv_setup = setup_test_kv_store()

    df_out = kv_setup["df"].with_column(
        "data", kv_get_with_name("item_id", kv_setup["emb_alias"], columns=["embedding"])
    )
    df_out = df_out.collect()
    pd = df_out.to_pydict()
    assert isinstance(pd["data"][0], dict)
    assert "embedding" in pd["data"][1]
    assert "metadata" not in pd["data"][1]
    assert pd["data"][2]["embedding"] == [2.0, 2.2]


def test_kv_get_selective_multi_columns():
    kv_setup = setup_test_kv_store()

    e1 = kv_get_with_name("item_id", kv_setup["emb_alias"], columns=["embedding"])
    e2 = kv_get_with_name("item_id", kv_setup["meta_alias"], columns=["metadata"])
    merge = daft.func(lambda a, b: {**(a or {}), **(b or {})}, return_dtype=daft.DataType.python())
    df_out = kv_setup["df"].with_column("data", merge(e1, e2)).collect()
    pd = df_out.to_pydict()
    assert isinstance(pd["data"][0], dict)
    assert set(pd["data"][0].keys()).issuperset({"embedding", "metadata"})
    assert pd["data"][1]["embedding"] == [1.0, 1.1]

    for i, expected in enumerate(EXPECTED_DATA):
        assert pd["data"][i]["embedding"] == expected["embedding"]
        assert pd["data"][i]["metadata"] == expected["metadata"]


def test_kv_get_equals_join_alignment():
    kv_setup = setup_test_kv_store()

    original_pd = kv_setup["df"].collect().to_pydict()

    e_emb = kv_get_with_name("item_id", kv_setup["emb_alias"], columns=["embedding"])
    e_meta = kv_get_with_name("item_id", kv_setup["meta_alias"], columns=["metadata"])

    merge = daft.func(lambda a, b: {**(a or {}), **(b or {})}, return_dtype=daft.DataType.python())
    df_merged = kv_setup["df"].with_column("data", merge(e_emb, e_meta)).collect()
    merged_pd = df_merged.to_pydict()

    # Verify merged data matches original data
    for i in range(len(original_pd["item_id"])):
        assert merged_pd["data"][i]["embedding"] == original_pd["embedding"][i]
        assert merged_pd["data"][i]["metadata"] == original_pd["metadata"][i]

    df_embeddings = kv_setup["df"].with_column("embedding_data", e_emb).select("item_id", "embedding_data")
    df_metadata = kv_setup["df"].with_column("metadata_data", e_meta).select("item_id", "metadata_data")
    df_joined = df_embeddings.join(df_metadata, on="item_id").collect()
    joined_pd = df_joined.to_pydict()

    # Verify joined data matches original data and merged data
    for i in range(len(joined_pd["item_id"])):
        item_id = int(joined_pd["item_id"][i])
        assert joined_pd["embedding_data"][i]["embedding"] == original_pd["embedding"][item_id]
        assert joined_pd["metadata_data"][i]["metadata"] == original_pd["metadata"][item_id]

        combined = {**(joined_pd["embedding_data"][i] or {}), **(joined_pd["metadata_data"][i] or {})}
        assert combined == merged_pd["data"][item_id]

    # Verify all data structures match expected data
    for i, expected in enumerate(EXPECTED_DATA):
        assert original_pd["embedding"][i] == expected["embedding"]
        assert original_pd["metadata"][i] == expected["metadata"]
        assert merged_pd["data"][i]["embedding"] == expected["embedding"]
        assert merged_pd["data"][i]["metadata"] == expected["metadata"]

        item_id = int(joined_pd["item_id"][i])
        if item_id == i:  # Only check when item_id matches index
            assert joined_pd["embedding_data"][i]["embedding"] == expected["embedding"]
            assert joined_pd["metadata_data"][i]["metadata"] == expected["metadata"]
