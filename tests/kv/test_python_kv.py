from __future__ import annotations

import time

import daft
from daft.expressions import col
from daft.functions.kv import kv_batch_get_with_name, kv_get_with_name


class MockKVStore:
    def __init__(self, name: str, data: dict):
        self.name = name
        self.backend_type = "python_mock"
        self._data = data
        self.call_counts = {"get": 0, "batch_get": 0}
        self.batch_get_sizes = []

    def get(self, key: str):
        self.call_counts["get"] += 1
        return self._data.get(key)

    def batch_get(self, keys: list[str]) -> list:
        self.call_counts["batch_get"] += 1
        self.batch_get_sizes.append(len(keys))
        return [self._data.get(k) for k in keys]

    def schema_fields(self) -> list[str]:
        # Inspect first item to infer fields
        if not self._data:
            return []
        first_val = next(iter(self._data.values()))
        if isinstance(first_val, dict):
            return list(first_val.keys())
        return []


def test_kv_get_execution():
    """Test get execution logic using a custom mock KV store."""
    mock_data = {
        "0": {"val": "v0"},
        "1": {"val": "v1"},
    }
    mock_store_name = f"mock_store_get_{int(time.time() * 1000)}"
    mock_store = MockKVStore(mock_store_name, mock_data)

    daft.attach_kv(mock_store, alias=mock_store_name)

    df = daft.from_pydict({"keys": ["0", "1"]})
    df_res = df.with_column("val", kv_get_with_name(mock_store_name, col("keys"), columns=["val"])).collect()

    res = df_res.to_pydict()
    assert len(res["val"]) == 2
    assert res["val"][0]["val"] == "v0"
    assert res["val"][1]["val"] == "v1"

    # Verify Mock Store calls - expects 2 individual calls since it's not batched
    assert mock_store.call_counts["get"] == 2


def test_kv_batch_get_execution():
    """Test batch_get execution logic using a custom mock KV store."""
    # Create mock data
    mock_data = {
        "0": {"val": "v0", "extra": 0},
        "1": {"val": "v1", "extra": 1},
        "2": {"val": "v2", "extra": 2},
        "3": {"val": "v3", "extra": 3},
        "4": {"val": "v4", "extra": 4},
    }
    mock_store_name = f"mock_store_{int(time.time() * 1000)}"
    mock_store = MockKVStore(mock_store_name, mock_data)

    # Attach the mock store
    daft.attach_kv(mock_store, alias=mock_store_name)

    # Create DataFrame with keys
    df = daft.from_pydict({"keys": ["0", "1", "2", "3", "4"]})

    # Test case 1: batch_size=2 (should trigger 3 calls: [2, 2, 1])
    batch_size = 2
    df_res = df.with_column(
        "val", kv_batch_get_with_name(mock_store_name, col("keys"), batch_size=batch_size, columns=["val"])
    ).collect()

    res = df_res.to_pydict()
    assert len(res["val"]) == 5
    for i in range(5):
        assert res["val"][i]["val"] == f"v{i}"

    # Verify Mock Store calls
    assert mock_store.call_counts["batch_get"] == 3
    assert mock_store.batch_get_sizes == [2, 2, 1]

    # Reset mock stats
    mock_store.call_counts["batch_get"] = 0
    mock_store.batch_get_sizes = []

    # Test case 2: batch_size=5 (should trigger 1 call: [5])
    batch_size = 5
    df_res_2 = df.with_column(
        "val", kv_batch_get_with_name(mock_store_name, col("keys"), batch_size=batch_size, columns=["val"])
    ).collect()

    res_2 = df_res_2.to_pydict()
    assert len(res_2["val"]) == 5
    for i in range(5):
        assert res_2["val"][i]["val"] == f"v{i}"

    assert mock_store.call_counts["batch_get"] == 1
    assert mock_store.batch_get_sizes == [5]
