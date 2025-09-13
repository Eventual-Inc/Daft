"""Tests for session-based KV Store functionality."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import daft
from daft.kv import load_kv


class TestSessionBasedKVStore:
    """Test session-based KV Store functionality."""

    def test_load_kv_factory_function(self):
        """Test the load_kv factory function."""
        # Test memory KV store (should always work)
        memory_kv = load_kv("memory", name="test_memory")
        assert memory_kv.name == "test_memory"
        assert memory_kv.backend_type == "memory"

        # Test with initial data
        memory_kv_with_data = load_kv("memory", name="test_with_data", initial_data={"key1": "value1"})
        assert memory_kv_with_data.name == "test_with_data"
        assert memory_kv_with_data.backend_type == "memory"

    def test_session_attach_and_detach(self):
        """Test attaching and detaching KV stores to session."""
        # Create a memory KV store
        memory_kv = load_kv("memory", name="test_session")

        # Test attach
        daft.attach(memory_kv, alias="test_alias")
        assert daft.has_kv("test_alias")

        # Test get
        retrieved_kv = daft.get_kv("test_alias")
        assert retrieved_kv.name == "test_session"
        assert retrieved_kv.backend_type == "memory"

        # Test detach
        daft.detach_kv("test_alias")
        assert not daft.has_kv("test_alias")

    def test_session_current_kv(self):
        """Test setting and getting current KV store.

        Note: The first attached KV store automatically becomes the current KV store.
        This is the expected behavior in Daft's session management.
        """
        # Ensure clean session state at the beginning
        daft.set_kv(None)

        # Clean up any existing KV stores from previous tests
        for alias in ["first", "second"]:
            try:
                daft.detach_kv(alias)
            except ValueError:
                pass  # KV store doesn't exist, which is fine

        # Verify clean initial state
        assert daft.current_kv() is None

        # Create memory KV stores
        kv1 = load_kv("memory", name="kv1")
        kv2 = load_kv("memory", name="kv2")

        # Attach first KV store - should automatically become current
        daft.attach(kv1, alias="first")
        current = daft.current_kv()
        assert current is not None
        assert current.name == "kv1"

        # Attach second KV store - first should remain current
        daft.attach(kv2, alias="second")
        current = daft.current_kv()
        assert current is not None
        assert current.name == "kv1"  # Still the first one

        # Test explicit setting to second
        daft.set_kv("second")
        current = daft.current_kv()
        assert current is not None
        assert current.name == "kv2"

        # Test setting back to first
        daft.set_kv("first")
        current = daft.current_kv()
        assert current is not None
        assert current.name == "kv1"

        # Test explicit clearing
        daft.set_kv(None)
        assert daft.current_kv() is None

        # Cleanup
        daft.detach_kv("first")
        daft.detach_kv("second")

    def test_kv_store_configurations(self):
        """Test KV store configurations."""
        # Test memory KV store
        memory_kv = load_kv("memory", name="config_test")
        config = memory_kv.get_config()
        assert config is not None

        # Test Lance KV store (if available)
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                lance_path = Path(temp_dir) / "test.lance"
                lance_kv = load_kv("lance", name="lance_test", uri=str(lance_path))
                config = lance_kv.get_config()
                assert config is not None
                assert config.is_lance_backend()
                assert config.get_active_backend() == "lance"
        except ImportError:
            pytest.skip("Lance not available")

        # Test LMDB KV store (if available)
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                lmdb_path = Path(temp_dir) / "test.lmdb"
                lmdb_kv = load_kv("lmdb", name="lmdb_test", path=str(lmdb_path))
                config = lmdb_kv.get_config()
                assert config is not None
                assert config.is_lmdb_backend()
                assert config.get_active_backend() == "lmdb"
        except ImportError:
            pytest.skip("LMDB not available")

    def test_session_based_kv_functions_without_kv(self):
        """Test that KV functions fail gracefully when no KV store is attached."""
        from daft.functions.kv import kv_batch_get, kv_exists, kv_get

        # Create a simple DataFrame
        df = daft.from_pydict({"row_id": [1, 2, 3]})

        # Ensure no KV store is set
        daft.set_kv(None)

        # Test kv_get
        with pytest.raises(ValueError, match="No KV store is currently attached"):
            df.with_column("data", kv_get("row_id"))

        # Test kv_batch_get
        with pytest.raises(ValueError, match="No KV store is currently attached"):
            df.with_column("data", kv_batch_get("row_id"))

        # Test kv_exists
        with pytest.raises(ValueError, match="No KV store is currently attached"):
            df.with_column("exists", kv_exists("row_id"))

    def test_memory_kv_store_operations(self):
        """Test memory KV store operations."""
        # Create memory KV store with initial data
        initial_data = {"key1": "value1", "key2": "value2"}
        memory_kv = load_kv("memory", name="ops_test", initial_data=initial_data)

        # Test data access
        assert memory_kv.data == initial_data
        assert memory_kv.size() == 2
        assert memory_kv.exists("key1")
        assert not memory_kv.exists("key3")

        # Test data manipulation
        memory_kv.set("key3", "value3")
        assert memory_kv.size() == 3
        assert memory_kv.get("key3") == "value3"

        # Test deletion
        assert memory_kv.delete("key1")
        assert not memory_kv.exists("key1")
        assert memory_kv.size() == 2

        # Test clear
        memory_kv.clear()
        assert memory_kv.size() == 0

    def test_kv_store_with_columns_configuration(self):
        """Test KV store with columns configuration."""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                lance_path = Path(temp_dir) / "test.lance"

                # Create Lance KV store with specific columns
                lance_kv = load_kv(
                    "lance", name="columns_test", uri=str(lance_path), columns=["col1", "col2"], batch_size=500
                )

                assert lance_kv.columns == ["col1", "col2"]
                assert lance_kv.batch_size == 500

                # Test with_columns method
                new_lance_kv = lance_kv.with_columns(["col3", "col4"])
                assert new_lance_kv.columns == ["col3", "col4"]
                assert new_lance_kv.batch_size == 500  # Should preserve other settings

                # Test with_batch_size method
                new_batch_kv = lance_kv.with_batch_size(1000)
                assert new_batch_kv.batch_size == 1000
                assert new_batch_kv.columns == ["col1", "col2"]  # Should preserve columns

        except ImportError:
            pytest.skip("Lance not available")

    def test_multiple_kv_stores_in_session(self):
        """Test managing multiple KV stores in a session."""
        # Create multiple KV stores
        memory_kv1 = load_kv("memory", name="multi1", initial_data={"a": 1})
        memory_kv2 = load_kv("memory", name="multi2", initial_data={"b": 2})
        memory_kv3 = load_kv("memory", name="multi3", initial_data={"c": 3})

        # Attach all to session
        daft.attach(memory_kv1, alias="store1")
        daft.attach(memory_kv2, alias="store2")
        daft.attach(memory_kv3, alias="store3")

        # Verify all are attached
        assert daft.has_kv("store1")
        assert daft.has_kv("store2")
        assert daft.has_kv("store3")

        # Test switching between stores
        daft.set_kv("store1")
        assert daft.current_kv().name == "multi1"

        daft.set_kv("store2")
        assert daft.current_kv().name == "multi2"

        daft.set_kv("store3")
        assert daft.current_kv().name == "multi3"

        # Cleanup
        daft.detach_kv("store1")
        daft.detach_kv("store2")
        daft.detach_kv("store3")

        # Verify cleanup
        assert not daft.has_kv("store1")
        assert not daft.has_kv("store2")
        assert not daft.has_kv("store3")

    def test_kv_store_error_handling(self):
        """Test KV store error handling."""
        # Test invalid store type
        with pytest.raises(ValueError, match="KV store type 'invalid' is not yet supported"):
            load_kv("invalid", name="test")

        # Test missing required parameters for Lance
        with pytest.raises(ValueError, match="uri is required for Lance KV store"):
            load_kv("lance", name="test")

        # Test missing required parameters for LMDB
        with pytest.raises(ValueError, match="path is required for LMDB KV store"):
            load_kv("lmdb", name="test")

        # Test getting non-existent KV store
        with pytest.raises(ValueError):
            daft.get_kv("non_existent")

        # Test detaching non-existent KV store
        with pytest.raises(ValueError):
            daft.detach_kv("non_existent")

    def test_kv_store_string_representations(self):
        """Test KV store string representations."""
        # Test memory KV store
        memory_kv = load_kv("memory", name="repr_test", initial_data={"key": "value"})
        assert "MemoryKVStore" in repr(memory_kv)
        assert "repr_test" in str(memory_kv)
        assert "1 items" in str(memory_kv)

        # Test Lance KV store (if available)
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                lance_path = Path(temp_dir) / "test.lance"
                lance_kv = load_kv("lance", name="lance_repr", uri=str(lance_path))
                assert "LanceKVStore" in repr(lance_kv)
                assert "lance_repr" in str(lance_kv)
                assert str(lance_path) in str(lance_kv)
        except ImportError:
            pytest.skip("Lance not available")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
