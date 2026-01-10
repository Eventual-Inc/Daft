from __future__ import annotations

import os
import shutil

import pytest

import daft
from daft import col
from daft.dependencies import pa
from daft.functions.kv import (
    kv_batch_get_with_name,
    kv_exists_with_name,
    kv_get_with_name,
)
from daft.kv.lance import LanceKVStore
from daft.session import Session

PYARROW_VERSION = tuple(int(part) for part in pa.__version__.split(".")[:2])
PYARROW_LOWER_BOUND_SKIP = PYARROW_VERSION < (9, 0)


def setup_lance_dataset(path):
    import lance

    if os.path.exists(path):
        shutil.rmtree(path)

    # Create a simple dataset
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5]),
            "val": pa.array(["a", "b", "c", "d", "e"]),
            "vector": pa.array(
                [[1.0, 0.0], [0.0, 1.0], [1.0, 1.0], [0.0, 0.0], [2.0, 2.0]], type=pa.list_(pa.float32(), 2)
            ),
        }
    )
    lance.write_dataset(data, path)


@pytest.mark.skipif(PYARROW_LOWER_BOUND_SKIP, reason="Lance tests require pyarrow >= 9.0.0")
class TestLanceKV:
    @pytest.fixture
    def lance_dataset(self, tmp_path):
        _ = pytest.importorskip("lance")
        path = str(tmp_path / "lance_kv")
        setup_lance_dataset(path)
        return path

    def test_lance_kv_basic(self, lance_dataset):
        """Test Lance KV basic functionality."""
        # Create session and attach
        with Session() as sess:
            store = LanceKVStore(name="my_store", uri=lance_dataset, key_column="id")
            sess.attach_kv(store)

            # Create DF
            df = daft.from_pydict({"k": [1, 3, 5]})

            # Add KV lookup
            df = df.with_column("v", col("k").kv.get("my_store", columns=["val"]))

            # Collect results - optimizer should automatically handle KV rewrites
            res = df.collect()

            # Verification
            res_dict = res.to_pydict()
            vals = res_dict["v"]

            assert len(vals) == 3
            assert vals[0]["val"] == "a"
            assert vals[1]["val"] == "c"
            assert vals[2]["val"] == "e"

    def test_lance_kv_get_with_name_rowid_path(self, tmp_path):
        """Test using _rowid as key."""
        lance = pytest.importorskip("lance")
        uri = str(tmp_path / "lance_kv_rowid")

        table = pa.table({"id": [0, 1, 2, 3], "value": ["a", "b", "c", "d"]})
        lance.write_dataset(table, uri)

        with Session() as sess:
            store = LanceKVStore(name="lance_rowid", uri=uri, key_column="_rowid")
            sess.attach_kv(store, alias="lance_rowid")

            df = daft.from_pydict({"key": [0, 2]})
            df = df.with_column(
                "data",
                kv_get_with_name("lance_rowid", col("key"), columns=["value"]),
            )
            result = df.collect().to_pydict()["data"]

            assert result == [{"value": "a"}, {"value": "c"}]

    def test_lance_kv_batch_get_with_name_id_path(self, tmp_path):
        """Test batch get functionality."""
        lance = pytest.importorskip("lance")
        uri = str(tmp_path / "lance_kv_id")

        table = pa.table({"id": [0, 1, 2, 3], "value": ["a", "b", "c", "d"]})
        lance.write_dataset(table, uri)

        with Session() as sess:
            store = LanceKVStore(name="lance_id", uri=uri, key_column="id")
            sess.attach_kv(store, alias="lance_id")

            df = daft.from_pydict({"key": [1, 3]})
            df = df.with_column(
                "data",
                kv_batch_get_with_name("lance_id", col("key"), columns=["value"], batch_size=32),
            )
            result = df.collect().to_pydict()["data"]

            assert isinstance(result, list)
            values = {row["value"] if row is not None else None for row in result}
            assert values == {"b", "d"}

    def test_lance_kv_exists_with_name(self, tmp_path):
        """Test exists functionality."""
        lance = pytest.importorskip("lance")
        uri = str(tmp_path / "lance_kv_exists")

        table = pa.table({"id": [0, 1, 2, 3], "value": ["a", "b", "c", "d"]})
        lance.write_dataset(table, uri)

        with Session() as sess:
            store = LanceKVStore(name="lance_exists", uri=uri, key_column="id")
            sess.attach_kv(store, alias="lance_exists")

            df = daft.from_pydict({"key": [0, 4]})
            df = df.with_column(
                "exists",
                kv_exists_with_name("lance_exists", col("key")),
            )
            exists = df.collect().to_pydict()["exists"]

            assert exists == [True, False]
